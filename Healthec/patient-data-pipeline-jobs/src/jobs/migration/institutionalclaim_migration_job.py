import os
from string import Template

import click
from fhirclient.resources.claim import Claim
from fhirclient.resources.condition import Condition
from fhirclient.resources.coverage import Coverage

# from fhirclient.resources.encounter import Encounter
from fhirclient.resources.organization import Organization
from fhirclient.resources.patient import Patient
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.procedure import Procedure
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window as w

from dependencies.spark import add_storage_context, start_spark
from utils.api_client import match_core_entity, post_core_entity, post_sub_entity
from utils.enums import ResourceType

# from utils.transformation import parse_date_time
from utils.utils import exit_with_error, load_config

CON_JINJA_TEMPLATE = "condition.j2"
PRC_JINJA_TEMPLATE = "procedure.j2"
CLM_JINJA_TEMPLATE = "claim.j2"


def _get_patient_match_attributes(value: str) -> str:
    attributes_template = Template(
        """
        {
            "identifier": [
                {
                    "type": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                "code": "RI",
                                "display": "Resource Identifier"
                            }
                        ]
                    },
                    "system": "http://healthec.com/identifier/member/health_record_key",
                    "value": "$health_record_key"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(health_record_key=value)


def transform_patient(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_patient_match_attributes(row_dict.get("HealthRecordkey")) if row_dict.get("HealthRecordkey") else ""
    )

    return Row(**row_dict)


def _get_coverage_match_attributes(value: str) -> str:
    attributes_template = Template(
        """
        {
            "identifier": [
                {
                    "type": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                "code": "MB",
                                "display": "Member Number"
                            }
                        ]
                    },
                    "system": "http://healthec.com/identifier/member/member_id",
                    "value": "$member_id"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(member_id=value)


def transform_coverage(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_coverage_match_attributes(row_dict.get("member_id")) if row_dict.get("member_id") else ""
    )

    return Row(**row_dict)


def _get_insurer_match_attributes(insurer_id: str, insurer_name: str) -> str:
    attributes_template = Template(
        """
        {
            "identifier": [
                {
                    "type": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                "code": "RI",
                                "display": "Resource Identifier"
                            }
                        ]
                    },
                    "system": "http://healthec.com/identifier/member/internal_id",
                    "value": "$insurer_id"
                }
            ],
            "name": "$insurer_name",
            "type": [
                {
                    "coding": [
                        {
                            "system": "http://hl7.org/fhir/ValueSet/organization-type",
                            "code": "pay",
                            "display": "Payer"
                        }
                    ],
                    "text": "Payer"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(insurer_id=insurer_id, insurer_name=insurer_name)


def transform_insurer(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_insurer_match_attributes(
            row_dict.get("insurance_company_id", ""), row_dict.get("insurance_company_name", "")
        )
        if row_dict.get("insurance_company_id") or row_dict.get("insurance_company_name")
        else ""
    )

    return Row(**row_dict)


def _get_provider_match_attributes(value: str) -> str:
    attributes_template = Template(
        """
        {
            "identifier": [
                {
                    "type": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                "code": "NPI"
                            }
                        ]
                    },
                    "system": "urn:oid:2.16.840.1.113883.4.6",
                    "value": "$provider_npi"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(provider_npi=value)


def transform_provider(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_provider_match_attributes(row_dict.get("provider_npi")) if row_dict.get("provider_npi") else ""
    )

    return Row(**row_dict)


def transform_condition(row: Row, transformer: FHIRTransformer) -> Row:
    transformer.load_template(CON_JINJA_TEMPLATE)
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["claim_id"] = data_dict.get("claim_id")
    response_dict["sequence"] = data_dict.get("sequence")
    response_dict["patient_id"] = data_dict.get("patient_id")

    data_dict["code"] = data_dict["diagnosis_code"]
    data_dict["code_system"] = data_dict["diagnosis_code_system"]

    data_dict.pop("claim_id")
    data_dict.pop("sequence")
    data_dict.pop("diagnosis_code")
    data_dict.pop("diagnosis_code_system")

    # render FHIR Condition resource
    resource = transformer.render_resource(ResourceType.Condition.value, data_dict)
    post_response = post_sub_entity(resource, Condition, data_dict["patient_id"], Patient)
    if "failure" in post_response:
        response_dict["condition_id"] = "failure"
    else:
        response_dict["condition_id"] = post_response

    return Row(**response_dict)


def transform_procedure(row: Row, transformer: FHIRTransformer) -> Row:
    transformer.load_template(PRC_JINJA_TEMPLATE)
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["claim_id"] = data_dict.get("claim_id")
    response_dict["sequence"] = data_dict.get("sequence")
    response_dict["patient_id"] = data_dict.get("patient_id")

    data_dict.pop("claim_id")
    data_dict.pop("sequence")

    # render FHIR Procedure resource
    resource = transformer.render_resource(ResourceType.Procedure.value, data_dict)
    post_response = post_sub_entity(resource, Procedure, data_dict["patient_id"], Patient)
    if "failure" in post_response:
        response_dict["procedure_id"] = "failure"
    else:
        response_dict["procedure_id"] = post_response

    return Row(**response_dict)


def transform_claim(row: Row, transformer: FHIRTransformer) -> Row:
    transformer.load_template(CLM_JINJA_TEMPLATE)
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["row_id"] = data_dict.get("row_id")

    data_dict.pop("row_id")

    # render FHIR Encounter resource
    resource = transformer.render_resource(ResourceType.Claim.value, data_dict)

    post_response = post_core_entity(resource, Claim)
    if "failure" in post_response:
        response_dict["post_response"] = post_response
    else:
        response_dict["post_response"] = "success"

    return Row(**response_dict)


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="institutionalclaim_migration_job",
)
@click.option(
    "--config-file-path",
    "-c",
    help="application config file path",
    default=None,
)
def main(app_name, config_file_path=None):
    app_name = os.environ.get("SPARK_APP_NAME", app_name)

    spark_config = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }

    # start spark session and logger
    spark, log = start_spark(app_name, spark_config)
    spark.sparkContext.addPyFile("/app/python-deps.zip")
    log.warn(f"spark app {app_name} started")

    # if config file is provided, extract details into a dict
    # config file should be a yaml file
    config = {}
    if config_file_path:
        config = load_config(config_file_path)
    log.info(f"config: {config}")

    try:
        # validate environment variables and config parameters
        data_file_path = os.environ.get(
            "DATA_FILE_PATH",
            config.get("data_file_path", ""),
        )
        if not data_file_path:
            exit_with_error(log, "data file path should be provided!")
        log.warn(f"data_file_path: {data_file_path}")

        error_file_path = os.environ.get(
            "ERROR_FILE_PATH",
            config.get("error_file_path", ""),
        )
        if not error_file_path:
            exit_with_error(log, "error file path should be provided!")
        log.warn(f"error_file_path: {error_file_path}")

        # add storage context in spark session
        spark = add_storage_context(spark, [data_file_path])

        # load the encounter data file
        log.warn("load encounter data file path into dataframe")
        src_df = spark.read.options(header=True).csv(data_file_path)

        # select claim level fields and add row_id
        claim_df = (
            src_df.select(
                [
                    "claim_id",
                    "ClaimType",
                    "member_id",
                    "member_mrn",
                    "HealthRecordkey",
                    "insurance_company_name",
                    "insurance_company_id",
                    "primary_payer_cd",
                    "claim_tob",
                    "admission_type",
                    "discharge_status",
                    "claim_billed_date",
                    "claim_start_date",
                    "claim_end_date",
                    "drg_code",
                    "drg_type",
                    "claim_total_charges",
                    "claim_adjudication_status",
                    "claim_total_paid",
                    "claim_paid_date",
                    "claim_deduct_amount",
                    "claim_copay_amount",
                    "claim_coinsurance_amount",
                    "claim_allowed_amount",
                    "claim_discount_amount",
                    "claim_patient_paid_amount",
                    "claim_other_payer_paid",
                ]
            )
            .drop_duplicates()
            .withColumn("row_id", f.expr("uuid()"))
        )
        claim_df.persist()

        # apply transformation on patient dataframe
        patient_df = claim_df.select("HealthRecordkey").drop_duplicates()
        patient_rdd = patient_df.rdd.map(lambda row: transform_patient(row)).persist()
        patient_df = spark.createDataFrame(patient_rdd)
        patient_df.persist()

        # register patient service udf
        pat_service_udf = f.udf(lambda df: match_core_entity(df, Patient))

        # get patient by healthrecordkey
        processed_patient_df = patient_df.withColumn("patient_id", pat_service_udf(patient_df["attributes"])).select(
            ["HealthRecordkey", "patient_id"]
        )
        processed_patient_df.persist()

        # apply transformation on coverage dataframe
        coverage_df = claim_df.select("member_id").drop_duplicates()
        coverage_rdd = coverage_df.rdd.map(lambda row: transform_coverage(row)).persist()
        coverage_df = spark.createDataFrame(coverage_rdd)
        coverage_df.persist()

        # register coverage service udf
        cov_service_udf = f.udf(lambda df: match_core_entity(df, Coverage))

        # get coverage by member id
        processed_coverage_df = patient_df.withColumn("coverage_id", cov_service_udf(coverage_df["attributes"])).select(
            ["member_id", "coverage_id"]
        )
        processed_coverage_df.persist()

        # apply transformation on insurer dataframe
        insurer_df = claim_df.select(["insurance_company_id", "insurance_company_name"]).drop_duplicates()
        insurer_rdd = insurer_df.rdd.map(lambda row: transform_insurer(row)).persist()
        insurer_df = spark.createDataFrame(insurer_rdd)
        insurer_df.persist()

        # register insurer service udf
        ins_service_udf = f.udf(lambda df: match_core_entity(df, Organization))

        # get insurer by member id
        processed_insurer_df = insurer_df.withColumn("insurer_id", ins_service_udf(insurer_df["attributes"])).select(
            ["insurance_company_id", "insurance_company_name", "insurer_id"]
        )
        processed_insurer_df.persist()

        # update patient id, coverage id, and insurer id on claim dataframe
        claim_df = (
            claim_df.join(processed_patient_df, on="HealthRecordkey", how="left")
            .join(processed_coverage_df, on="member_id", how="left")
            .join(processed_insurer_df, on=["insurance_company_id", "insurance_company_name"], how="left")
        )
        claim_df.persist()

        # collect line diagnosis codes
        line_diag_df = (
            src_df.withColumn(
                "diagnosis_code_list",
                f.array(
                    f.col("primary_diagnosis"),
                    f.col("admitting_diagnosis"),
                    f.col("diagnosis_code_1"),
                    f.col("diagnosis_code_2"),
                    f.col("diagnosis_code_3"),
                    f.col("diagnosis_code_4"),
                ),
            )
            .withColumn(
                "diagnosis_code_list", f.array_distinct(f.expr("filter(diagnosis_code_list, x -> x is not null)"))
            )
            .withColumn("diagnosis_code", f.explode(f.col("diagnosis_code_list")))
            .withColumn(
                "diagnosis_code_system",
                f.when(f.col("icd_version") == "0", f.lit("http://hl7.org/fhir/sid/icd-10-cm"))
                .when(f.col("icd_version") == "9", f.lit("http://hl7.org/fhir/sid/icd-9-cm"))
                .otherwise(f.lit("https://bluebutton.cms.gov/resources/codesystem/diagnosis-type")),
            )
            .select(["claim_id", "line_number", "diagnosis_code", "diagnosis_code_system"])
        )
        line_diag_df.persist()
        # consolidate claim diagnoses
        claim_diag_df = line_diag_df.select(["claim_id", "diagnosis_code", "diagnosis_code_system"]).drop_duplicates()
        diag_window_spec = w.partitionBy("claim_id").orderBy("claim_id")
        claim_diag_seq_df = claim_diag_df.withColumn("sequence", f.row_number().over(diag_window_spec))
        claim_diag_seq_df.persist()
        # update diagnoses sequences at line level
        line_diag_seq_df = line_diag_df.join(
            claim_diag_seq_df,
            (line_diag_df.claim_id == claim_diag_seq_df.claim_id)
            & (line_diag_df.diagnosis_code == claim_diag_seq_df.diagnosis_code),
        ).select(line_diag_df["*"], claim_diag_seq_df["sequence"])
        line_diag_seq_df = (
            line_diag_seq_df.groupBy(["claim_id", "line_number"])
            .agg(f.collect_list("sequence").alias("diag_sequences"))
            .select(["claim_id", "line_number", "diag_sequences"])
        )
        line_diag_seq_df.persist()
        claim_diag_seq_df = claim_diag_seq_df.join(claim_df, on="claim_id").select(
            claim_diag_seq_df["*"], claim_df["patient_id"]
        )
        # initialize fhirtransformer
        transformer = FHIRTransformer()
        # post diagnosis and collect condition ids
        claim_diag_final_rdd = claim_diag_seq_df.rdd.map(lambda row: transform_condition(row, transformer)).persist()
        claim_diag_final_df = spark.createDataFrame(claim_diag_final_rdd).filter(f.col("condition_id") != "failure")
        claim_diag_final_df.persist()

        # collect line procedure codes
        line_proc_df = src_df.withColumnRenamed("principal_procedure_code", "procedure_code").select(
            ["claim_id", "line_number", "procedure_code"]
        )
        claim_proc_df = (
            line_proc_df.groupBy("claim_id")
            .agg(f.collect_list("procedure_code").alias("procedure_code_list"))
            .withColumn("procedure_code", f.explode(f.array_distinct(f.col("procedure_code_list"))))
            .select(["claim_id", "procedure_code"])
        )
        proc_window_spec = w.partitionBy("claim_id").orderBy("claim_id")
        claim_proc_seq_df = claim_proc_df.withColumn("sequence", f.row_number().over(proc_window_spec))
        claim_proc_seq_df.persist()
        line_proc_seq_df = line_proc_df.join(
            claim_proc_seq_df,
            (line_proc_df.claim_id == claim_proc_seq_df.claim_id)
            & (line_proc_df.procedure_code == claim_proc_seq_df.procedure_code),
        ).select(line_proc_df["*"], claim_proc_seq_df["sequence"])
        line_proc_seq_df = (
            line_proc_seq_df.groupBy(["claim_id", "line_number"])
            .agg(f.collect_list("sequence").alias("proc_sequences"))
            .select(["claim_id", "line_number", "proc_sequences"])
        )
        line_proc_seq_df.persist()
        # post procedure and collect procedure ids
        claim_proc_final_rdd = claim_proc_seq_df.rdd.map(lambda row: transform_procedure(row, transformer)).persist()
        claim_proc_final_df = spark.createDataFrame(claim_proc_final_rdd).filter(f.col("procedure_id") != "failure")
        claim_proc_final_df.persist()

        # collect supporting info
        sup_info_schema = StructType(
            [
                StructField("code", StringType(), True),
                StructField("code_system", StringType(), True),
                StructField("code_display", StringType(), True),
                StructField("value_string", StringType(), True),
            ]
        )
        sup_info_df = (
            claim_df.withColumn(
                "sup_info_1",
                f.when(
                    f.col("member_mrn").isNotNull(),
                    f.struct(
                        f.lit("medicalrecordnumber").alias("code"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType").alias("code_system"),
                        f.lit("Medical Record Number").alias("code_display"),
                        f.col("member_mrn").alias("value_string"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_2",
                f.when(
                    f.col("claim_tob").isNotNull(),
                    f.struct(
                        f.lit("typeofbill").alias("code"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType").alias("code_system"),
                        f.lit("Type of Bill").alias("code_display"),
                        f.col("claim_tob").alias("value_string"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_3",
                f.when(
                    f.col("admission_type").isNotNull(),
                    f.struct(
                        f.lit("admtype").alias("code"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType").alias("code_system"),
                        f.lit("Admission Type").alias("code_display"),
                        f.col("admission_type").alias("value_string"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_4",
                f.when(
                    f.col("discharge_status").isNotNull(),
                    f.struct(
                        f.lit("discharge-status").alias("code"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType").alias("code_system"),
                        f.lit("Discharge Status").alias("code_display"),
                        f.col("discharge_status").alias("value_string"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_5",
                f.when(
                    f.col("drg_code").isNotNull(),
                    f.struct(
                        f.lit("drg").alias("code"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType").alias("code_system"),
                        f.lit("DRG").alias("code_display"),
                        f.col("drg_code").alias("value_string"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info",
                f.array(
                    f.col("sup_info_1"),
                    f.col("sup_info_2"),
                    f.col("sup_info_3"),
                    f.col("sup_info_4"),
                    f.col("sup_info_5"),
                ),
            )
            .withColumn("sup_info", f.array_distinct(f.expr("filter(sup_info, x -> x is not null)")))
            .select(["claim_id", "sup_info"])
        )
        sup_info_df = sup_info_df.select(f.col("claim_id"), f.explode(f.col("sup_info")).alias("sup_info")).select(
            f.col("claim_id"),
            f.col("sup_info.code").alias("code"),
            f.col("sup_info.code_system").alias("code_system"),
            f.col("sup_info.code_display").alias("code_display"),
            f.col("sup_info.value_string").alias("value_string"),
        )
        sup_info_window_spec = w.partitionBy("claim_id").orderBy("claim_id")
        claim_sup_info_seq_df = sup_info_df.withColumn("sequence", f.row_number().over(sup_info_window_spec)).select(
            ["claim_id", "sequence", "code", "code_system", "code_display", "value_string"]
        )
        claim_sup_info_seq_df = claim_sup_info_seq_df.groupBy("claim_id").agg(
            f.collect_list(
                f.struct(
                    f.col("sequence"),
                    f.col("code"),
                    f.col("code_system"),
                    f.col("code_display"),
                    f.col("value_string"),
                )
            ).alias("sup_info")
        )
        claim_sup_info_seq_df.persist()

        # combine modifiers at line level
        line_mod_df = (
            src_df.withColumn(
                "modifier_code_list",
                f.array(
                    f.col("svc_modifier_1"),
                    f.col("svc_modifier_2"),
                    f.col("svc_modifier_3"),
                    f.col("svc_modifier_4"),
                ),
            )
            .withColumn("modifier_code_list", f.array_distinct(f.expr("filter(modifier_code_list, x -> x is not null)")))
            .withColumn("modifier_code", f.explode(f.col("modifier_code_list")))
            .withColumn("modifier_system", f.lit("https://bluebutton.cms.gov/resources/codesystem/hcpcs"))
            .select(["claim_id", "line_number", "modifier_code", "modifier_system"])
        )
        line_mod_df = (
            line_mod_df.groupBy(["claim_id", "line_number"])
            .agg(f.collect_list(f.struct(f.col("modifier_code"), f.col("modifier_system"))).alias("svc_modifiers"))
            .select(["claim_id", "line_number", "svc_modifiers"])
        )

        # collect careteam
        care_team_schema = StructType(
            [
                StructField("provider_npi", StringType(), True),
                StructField("provider_source_id", StringType(), True),
                StructField("provider_last_name", StringType(), True),
                StructField("provider_first_name", StringType(), True),
                StructField("provider_role", StringType(), True),
                StructField("provider_role_system", StringType(), True),
            ]
        )
        line_careteam_df = (
            src_df.withColumn(
                "care_team_1",
                f.when(
                    f.col("attending_npi").isNotNull(),
                    f.struct(
                        f.col("attending_npi").alias("provider_npi"),
                        f.col("attending_internal_id").alias("provider_source_id"),
                        f.col("attending_last_name").alias("provider_last_name"),
                        f.col("attending_first_name").alias("provider_first_name"),
                        f.lit("attending").alias("provider_role"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole").alias(
                            "provider_role_system"
                        ),
                    ),
                ).otherwise(f.lit(None).cast(care_team_schema)),
            )
            .withColumn(
                "care_team_2",
                f.when(
                    f.col("referring_npi").isNotNull(),
                    f.struct(
                        f.col("referring_npi").alias("provider_npi"),
                        f.col("referring_internal_id").alias("provider_source_id"),
                        f.col("referring_last_name").alias("provider_last_name"),
                        f.col("referring_first_name").alias("provider_first_name"),
                        f.lit("referring").alias("provider_role"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole").alias(
                            "provider_role_system"
                        ),
                    ),
                ).otherwise(f.lit(None).cast(care_team_schema)),
            )
            .withColumn(
                "care_team_3",
                f.when(
                    f.col("render_prescriber_npi").isNotNull(),
                    f.struct(
                        f.col("render_prescriber_npi").alias("provider_npi"),
                        f.col("render_prescriber_internal_id").alias("provider_source_id"),
                        f.col("render_prescriber_last_name").alias("provider_last_name"),
                        f.col("render_prescriber_first_name").alias("provider_first_name"),
                        f.lit("prescribing").alias("provider_role"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole").alias(
                            "provider_role_system"
                        ),
                    ),
                ).otherwise(f.lit(None).cast(care_team_schema)),
            )
            .withColumn(
                "care_team",
                f.array(f.col("care_team_1"), f.col("care_team_2"), f.col("care_team_3")),
            )
            .withColumn("care_team", f.array_distinct(f.expr("filter(care_team, x -> x is not null)")))
            .select(["claim_id", "line_number", "care_team"])
        )
        line_careteam_df = line_careteam_df.select(
            f.col("claim_id"), f.col("line_number"), f.explode(f.col("care_team")).alias("care_team")
        ).select(
            f.col("claim_id"),
            f.col("line_number"),
            f.col("care_team.provider_npi").alias("provider_npi"),
            f.col("care_team.provider_source_id").alias("provider_source_id"),
            f.col("care_team.provider_last_name").alias("provider_last_name"),
            f.col("care_team.provider_first_name").alias("provider_first_name"),
            f.col("care_team.provider_role").alias("provider_role"),
            f.col("care_team.provider_role_system").alias("provider_role_system"),
        )

        claim_careteam_df = line_careteam_df.select(
            [
                "claim_id",
                "provider_npi",
                "provider_source_id",
                "provider_last_name",
                "provider_first_name",
                "provider_role",
                "provider_role_system",
            ]
        ).drop_duplicates()
        careteam_window_spec = w.partitionBy("claim_id").orderBy("claim_id")
        claim_careteam_seq_df = claim_careteam_df.withColumn("sequence", f.row_number().over(careteam_window_spec))
        claim_careteam_seq_df.persist()

        line_careteam_seq_df = line_careteam_df.join(
            claim_careteam_seq_df,
            (line_careteam_df.claim_id == claim_careteam_seq_df.claim_id)
            & (line_careteam_df.provider_npi == claim_careteam_seq_df.provider_npi)
            & (line_careteam_df.provider_role == claim_careteam_seq_df.provider_role),
        ).select(line_careteam_df["*"], claim_careteam_seq_df["sequence"])
        line_careteam_seq_df = (
            line_careteam_seq_df.groupBy(["claim_id", "line_number"])
            .agg(f.collect_list("sequence").alias("careteam_sequences"))
            .select(["claim_id", "line_number", "careteam_sequences"])
        )
        line_careteam_seq_df.persist()

        # apply transformation on provider dataframe
        provider_df = (
            claim_careteam_seq_df.select("provider_npi").filter(f.col("provider_npi").isNotNull()).drop_duplicates()
        )
        provider_rdd = provider_df.rdd.map(lambda row: transform_provider(row)).persist()
        provider_df = spark.createDataFrame(provider_rdd)
        provider_df.persist()

        # register provider service udf
        prov_service_udf = f.udf(lambda df: match_core_entity(df, Practitioner))

        # get coverage by member id
        processed_provider_df = provider_df.withColumn(
            "coverage_id", prov_service_udf(provider_df["attributes"])
        ).select(["provider_npi", "provider_id"])
        processed_provider_df.persist()

        claim_careteam_seq_df = claim_careteam_seq_df.join(processed_provider_df, on="provider_npi", how="left")

        line_df = (
            src_df.join(line_mod_df, on=["claim_id", "line_number"], how="left")
            .join(line_diag_seq_df, on=["claim_id", "line_number"], how="left")
            .join(line_proc_seq_df, on=["claim_id", "line_number"], how="left")
            .join(line_careteam_seq_df, on=["claim_id", "line_number"], how="left")
            .select(
                [
                    "claim_id",
                    "line_number",
                    "diag_sequences",
                    "proc_sequences",
                    "careteam_sequences",
                    "service_start_date",
                    "service_end_date",
                    "revenue_code",
                    "cpt_code",
                    "cpt_code_description",
                    "hcpcs_code",
                    "hcpcs_code_description",
                    "svc_modifiers",
                    "service_units",
                    "place_of_service",
                    "type_of_service",
                    "svc_line_charges",
                    "line_amount_paid",
                    "svc_line_paid_date",
                    "line_deduct_amount",
                    "line_copay_amount",
                    "line_coinsurance_amount",
                    "line_allowed_amount",
                    "line_discount_amount",
                    "line_patient_paid_amount",
                    "line_other_payer_paid",
                    "medicare_paid_amt",
                    "mco_paid_amount",
                ]
            )
        )
        claim_line_df = (
            line_df.groupBy("claim_id")
            .agg(
                f.collect_list(
                    f.struct(
                        f.col("line_number"),
                        f.col("diag_sequences"),
                        f.col("proc_sequences"),
                        f.col("careteam_sequences"),
                        f.col("service_start_date"),
                        f.col("service_end_date"),
                        f.col("revenue_code"),
                        f.col("cpt_code"),
                        f.col("cpt_code_description"),
                        f.col("hcpcs_code"),
                        f.col("hcpcs_code_description"),
                        f.col("svc_modifiers"),
                        f.col("service_units"),
                        f.col("place_of_service"),
                        f.col("type_of_service"),
                        f.col("svc_line_charges"),
                        f.col("line_amount_paid"),
                        f.col("svc_line_paid_date"),
                        f.col("line_deduct_amount"),
                        f.col("line_copay_amount"),
                        f.col("line_coinsurance_amount"),
                        f.col("line_allowed_amount"),
                        f.col("line_discount_amount"),
                        f.col("line_patient_paid_amount"),
                        f.col("line_other_payer_paid"),
                        f.col("medicare_paid_amt"),
                        f.col("mco_paid_amount"),
                    )
                ).alias("service_lines")
            )
            .select(["claim_id", "service_lines"])
        )
        claim_line_df.persist()

        # construct claim resource
        claim_final_df = (
            claim_df
            # .join(processed_patient_df, on="member_id", how="left")
            # .join(processed_insurer_df, on=["insurance_company_id", "insurance_company_name"])
            .join(claim_diag_final_df, on="claim_id", how="left")
            .join(claim_proc_final_df, on="claim_id", how="left")
            .join(claim_sup_info_seq_df, on="claim_id", how="left")
            .join(claim_careteam_seq_df, on="claim_id", how="left")
            .join(claim_line_df, on="claim_id", how="left")
        )
        claim_final_df.persist()

        response_rdd = claim_final_df.rdd.map(lambda row: transform_claim(row, transformer)).persist()
        response_df = spark.createDataFrame(response_rdd)

        claim_processed = response_df.count()
        error_df = response_df.filter(f.col("post_response") != "success")
        error_df.persist()
        claim_failure = error_df.count()

        if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of claim processed: {claim_processed}")
        log.warn(f"total number of claim success: {(claim_processed - claim_failure)}")
        log.warn(f"total number of claim failure: {claim_failure}")
        log.warn(f"spark job {app_name} completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
