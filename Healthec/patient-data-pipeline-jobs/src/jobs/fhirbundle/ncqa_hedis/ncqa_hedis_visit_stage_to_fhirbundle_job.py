import json
import os
import sys
from datetime import datetime

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w

from dependencies.spark import add_storage_context, start_spark
from utils.constants import (
    NCQA_CLAIM_ID_SRC_SYSTEM,
    NCQA_MEMBER_ID_SRC_SYSTEM,
    NCQA_PROVIDER_ID_SRC_SYSTEM,
    SUPPLEMENTAL_DATA_URL,
    SUPPLEMENTAL_DATA_VALUE_BOOLEAN,
    NCQA_SOURCE_FILE_SYSTEM,
)
from utils.enums import ResourceType
from utils.transformation import parse_code_system, parse_date_time
from utils.utils import exit_with_error, load_config, upload_bundle_files

PAT_JINJA_TEMPLATE = "patient.j2"
COV_JINJA_TEMPLATE = "coverage.j2"
PRAT_JINJA_TEMPLATE = "practitioner_role.j2"
PRAT_ROLE_JINJA_TEMPLATE = "practitioner_role.j2"
ENC_JINJA_TEMPLATE = "encounter.j2"
CON_JINJA_TEMPLATE = "condition.j2"
PRC_JINJA_TEMPLATE = "procedure.j2"
CLM_JINJA_TEMPLATE = "claim.j2"
CLM_RESP_JINJA_TEMPLATE = "claim_response.j2"


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


def fhir_mapper_df(df: DataFrame) -> DataFrame:
    df = df.select(
        [
            "*",
            f.map_from_arrays(
                f.array(
                    f.lit("internal_id"),
                    f.lit("member_id"),
                    f.lit("member_system"),
                    f.lit("assigner_organization_id"),
                ),
                f.array(
                    f.col("patient_internal_id"),
                    f.col("member_id"),
                    f.lit(NCQA_MEMBER_ID_SRC_SYSTEM),
                    f.col("src_organization_id"),
                ),
            ).alias("patient_rsc"),
            f.map_from_arrays(
                f.array(f.lit("internal_id"), f.lit("source_id"), f.lit("source_system")),
                f.array(f.col("practitioner_internal_id"), f.col("provider_id"), f.lit(NCQA_PROVIDER_ID_SRC_SYSTEM)),
            ).alias("practitioner_rsc"),
            f.map_from_arrays(
                f.array(
                    f.lit("patient_internal_id"),
                    f.lit("encounter_internal_id"),
                    f.lit("procedure_internal_id"),
                    f.lit("code"),
                    f.lit("icd_identifier"),
                    f.lit("performed_date_time"),
                ),
                f.array(
                    f.col("patient_internal_id"),
                    f.col("encounter_internal_id"),
                    f.expr("uuid()"),
                    f.col("cpt_ii"),
                    f.lit("CPT"),
                    f.col("date_of_service"),
                ),
            ).alias("cpt_procedure_rsc"),
            # f.map_from_arrays(
            #     f.array(
            #         f.lit("internal_id"),
            #         f.lit("member_id"),
            #         f.lit("member_system"),
            #         f.lit("assigner_organization_id")
            #     ),
            #     f.array(
            #         f.col("coverage_internal_id"),
            #         f.col("member_id"),
            #         f.lit(NCQA_MEMBER_ID_SRC_SYSTEM),
            #         f.col("src_organization_id")
            #     ),
            # ).alias("coverage_rsc"),
        ]
    )
    return df


def _transform_encounter(row_dict: dict, transformer: FHIRTransformer) -> str:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("encounter_internal_id", "")
    data_dict["source_file_id"] = "visit"
    data_dict["source_file_system"] = NCQA_SOURCE_FILE_SYSTEM
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    # data_dict["practitioner_role_id"] = row_dict.get("encounter_practitioner_role_id", "")
    data_dict["practitioner_id"] = row_dict.get("practitioner_internal_id", "")

    if row_dict.get("admission_date", "") or row_dict.get("discharge_date", ""):
        data_dict["class_code"] = "IMP"
        data_dict["class_display"] = "inpatient encounter"

        data_dict["period_start_date"] = (
            parse_date_time(row_dict.get("admission_date")) if row_dict.get("admission_date") else ""
        )
        data_dict["period_end_date"] = (
            parse_date_time(row_dict.get("discharge_date")) if row_dict.get("discharge_date") else ""
        )
    elif row_dict.get("date_of_service", ""):
        # encounter is "ambulatory" if admission, discharge date not exists
        data_dict["class_code"] = "AMB"
        data_dict["class_display"] = "ambulatory"

        service_date_time = parse_date_time(row_dict.get("date_of_service")) if row_dict.get("date_of_service") else ""
        data_dict["period_start_date"] = service_date_time
        data_dict["period_end_date"] = service_date_time

    if row_dict.get("cms_place_of_service"):
        data_dict["physical_type_code"] = row_dict.get("cms_place_of_service", "")
        data_dict[
            "physical_type_system"
        ] = "https://www.cms.gov/Medicare/Coding/place-of-service-codes/Place_of_Service_Code_Set"

    # parse encounter status
    data_dict["status"] = "finished"

    # parse encounter type code
    if row_dict.get("cpt", ""):
        data_dict["type_code"] = row_dict.get("cpt", "")
        data_dict["type_system"] = parse_code_system("C")
    elif row_dict.get("hcpcs", ""):
        data_dict["type_code"] = row_dict.get("hcpcs", "")
        data_dict["type_system"] = parse_code_system("H")

    return _transform_resource(
        data_dict=data_dict,
        resource_type=ResourceType.Encounter.value,
        template=ENC_JINJA_TEMPLATE,
        transformer=transformer,
    )


def _transform_condition(row_dict: dict, transformer: FHIRTransformer) -> str:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("condition_internal_id", "")
    data_dict["source_file_id"] = "visit"
    data_dict["source_file_system"] = NCQA_SOURCE_FILE_SYSTEM
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["encounter_id"] = row_dict.get("encounter_internal_id", "")
    data_dict["onset_date_time"] = (
        parse_date_time(row_dict.get("onset_date_time")) if row_dict.get("onset_date_time") else ""
    )
    data_dict["code"] = row_dict.get("code", "")
    data_dict["code_system"] = (
        parse_code_system(row_dict.get("icd_identifier")) if row_dict.get("icd_identifier") else ""
    )

    return _transform_resource(
        data_dict=data_dict,
        resource_type=ResourceType.Condition.value,
        template=CON_JINJA_TEMPLATE,
        transformer=transformer,
    )


def _transform_procedure(row_dict: dict, transformer: FHIRTransformer) -> str:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("procedure_internal_id", "")
    data_dict["source_file_id"] = "visit"
    data_dict["source_file_system"] = NCQA_SOURCE_FILE_SYSTEM
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["encounter_id"] = row_dict.get("encounter_internal_id", "")
    data_dict["performed_date_time"] = (
        parse_date_time(row_dict.get("performed_date_time")) if row_dict.get("performed_date_time") else ""
    )
    data_dict["code"] = row_dict.get("code", "")
    data_dict["code_system"] = (
        parse_code_system(row_dict.get("icd_identifier")) if row_dict.get("icd_identifier") else ""
    )

    return _transform_resource(
        data_dict=data_dict,
        resource_type=ResourceType.Procedure.value,
        template=PRC_JINJA_TEMPLATE,
        transformer=transformer,
    )


def _transform_claim(row_dict: dict, transformer: FHIRTransformer) -> str:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("claim_internal_id", "")
    data_dict["source_file_id"] = "visit"
    data_dict["source_file_system"] = NCQA_SOURCE_FILE_SYSTEM
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["provider_practitioner_id"] = row_dict.get("practitioner_internal_id", "")
    data_dict["source_id"] = row_dict.get("claim_source_id", "")
    data_dict["source_system"] = NCQA_CLAIM_ID_SRC_SYSTEM
    data_dict["use"] = "claim"

    # # insurance
    # data_dict["insurance_coverage_id"] = row_dict.get("claim_coverage_id", "")
    # data_dict["insurance_sequence"] = 1
    # data_dict["insurance_focal"] = "true"

    # parse supplemental data
    if row_dict.get("supplemental_data") == "Y":
        data_dict["extensions"] = [
            {"value_extensions": [{"url": SUPPLEMENTAL_DATA_URL}, {"value_boolean": SUPPLEMENTAL_DATA_VALUE_BOOLEAN}]}
        ]

    # default claim type 'institutional'
    data_dict["type_code"] = "institutional"
    data_dict["type_system"] = "http://terminology.hl7.org/CodeSystem/claim-type"
    data_dict["type_display"] = "Institutional"
    data_dict["type_text"] = "Institutional"
    data_dict["created_date_time"] = (
        parse_date_time(row_dict.get("date_of_service"))
        if row_dict.get("date_of_service")
        else datetime.utcnow().isoformat()
    )
    diagnoses = []
    diagnosis_structs = row_dict.get("diagnosis_structs", [])
    if diagnosis_structs:
        for struct in diagnosis_structs:
            diagnoses.append(
                {
                    "sequence": int(struct.get("sequence")),
                    "diagnosis_condition_id": str(struct.get("condition_internal_id")),
                }
            )
    data_dict["diagnoses"] = diagnoses
    procedures = []
    procedure_structs = row_dict.get("procedure_structs", [])
    if procedure_structs:
        for struct in procedure_structs:
            procedures.append(
                {"sequence": int(struct.get("sequence")), "procedure_id": str(struct.get("procedure_internal_id"))}
            )
    data_dict["procedures"] = procedures

    diag_sequences_str = ""
    diag_sequences = row_dict.get("diag_sequences", [])
    if diag_sequences:
        diag_sequences_str = ",".join(map(str, diag_sequences))
    proc_sequences_str = ""
    proc_sequences = row_dict.get("proc_sequences", [])
    if proc_sequences:
        proc_sequences_str = ",".join(map(str, proc_sequences))

    line_item = {
        "sequence": 1,
        "diagnosis_sequence": diag_sequences_str,
        "procedure_sequence": proc_sequences_str,
        "service_date_time": parse_date_time(row_dict.get("date_of_service")) if row_dict.get("date_of_service") else "",
    }
    if row_dict.get("ub_revenue"):
        line_item["revenue_code"] = row_dict.get("ub_revenue")
    if row_dict.get("cms_place_of_service"):
        line_item["location_code"] = row_dict.get("cms_place_of_service")
        line_item[
            "location_system"
        ] = "https://www.cms.gov/Medicare/Coding/place-of-service-codes/Place_of_Service_Code_Set"
    data_dict["service_lines"] = [line_item]

    # update claim status
    data_dict["status"] = "cancelled"
    if row_dict.get("claim_status") == "1":
        data_dict["status"] = "active"

    return _transform_resource(
        data_dict=data_dict,
        resource_type=ResourceType.Claim.value,
        template=CLM_JINJA_TEMPLATE,
        transformer=transformer,
    )


def _transform_claim_response(row_dict: dict, transformer: FHIRTransformer) -> str:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("claim_response_internal_id", "")
    data_dict["source_file_id"] = "visit"
    data_dict["source_file_system"] = NCQA_SOURCE_FILE_SYSTEM
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["claim_id"] = row_dict.get("claim_internal_id", "")
    data_dict["use"] = "claim"
    data_dict["created_date_time"] = (
        parse_date_time(row_dict.get("date_of_service"))
        if row_dict.get("date_of_service")
        else datetime.utcnow().isoformat()
    )
    if row_dict.get("claim_status", "") == "1":
        data_dict["outcome"] = "complete"
        data_dict["status"] = "active"
    elif row_dict.get("claim_status", "") == "2":
        data_dict["outcome"] = "error"
        data_dict["status"] = "cancelled"

    data_dict["requestor_practitioner_id"] = row_dict.get("practitioner_internal_id", "")

    return _transform_resource(
        data_dict=data_dict,
        resource_type=ResourceType.ClaimResponse.value,
        template=CLM_RESP_JINJA_TEMPLATE,
        transformer=transformer,
    )


def render_resources(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    resources = []
    # render resources
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("patient_rsc"),
            resource_type=ResourceType.Patient.value,
            template=PAT_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )
    # resources.append(
    #     _transform_resource(
    #         data_dict=row_dict.get("coverage_rsc"),
    #         resource_type=ResourceType.Coverage.value,
    #         template=PRAT_JINJA_TEMPLATE,
    #         transformer=transformer,
    #     )
    # )

    if row_dict.get("practitioner_rsc", {}).get("source_id"):
        resources.append(
            _transform_resource(
                data_dict=row_dict.get("practitioner_rsc"),
                resource_type=ResourceType.Practitioner.value,
                template=PRAT_JINJA_TEMPLATE,
                transformer=transformer,
            )
        )

    resources.append(_transform_encounter(row_dict=row_dict, transformer=transformer))

    # condition
    if row_dict.get("diagnosis_structs"):
        for cond_dict in row_dict.get("diagnosis_structs"):
            if cond_dict.get("code"):
                cond_dict.update(
                    {
                        "patient_internal_id": row_dict.get("patient_internal_id"),
                        "encounter_internal_id": row_dict.get("encounter_internal_id"),
                    }
                )
                resources.append(_transform_condition(row_dict=cond_dict, transformer=transformer))

    # procedure
    if row_dict.get("procedure_structs"):
        for proc_dict in row_dict.get("procedure_structs"):
            if proc_dict.get("code"):
                proc_dict.update(
                    {
                        "patient_internal_id": row_dict.get("patient_internal_id"),
                        "encounter_internal_id": row_dict.get("encounter_internal_id"),
                    }
                )
                resources.append(_transform_procedure(row_dict=proc_dict, transformer=transformer))

    if row_dict.get("cpt_procedure_rsc", {}).get("code"):
        resources.append(_transform_procedure(row_dict=row_dict.get("cpt_procedure_rsc"), transformer=transformer))

    resources.append(_transform_claim(row_dict=row_dict, transformer=transformer))

    resources.append(_transform_claim_response(row_dict=row_dict, transformer=transformer))

    return Row(
        **{
            "resource_bundle": json.dumps(
                {
                    "resourceType": "Bundle",
                    "id": row_dict.get("bundle_id"),
                    "type": "batch",
                    "entry": resources,
                }
            )
        }
    )


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="ncqa_hedis_visit_stage_to_fhirbundle_job",
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
        delta_schema_location = os.environ.get(
            "DELTA_SCHEMA_LOCATION",
            config.get("delta_schema_location", ""),
        )
        if not delta_schema_location:
            exit_with_error(
                log,
                "delta schema location should be provided!",
            )

        delta_table_name = os.environ.get(
            "DELTA_TABLE_NAME",
            config.get("delta_table_name", "ncqa_hedis_visit"),
        )
        if not delta_table_name:
            exit_with_error(
                log,
                "delta table location should be provided!",
            )

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_table_name)
        log.warn(f"delta_table_location: {delta_table_location}")

        file_batch_id = os.environ.get("FILE_BATCH_ID", config.get("file_batch_id", ""))
        if not file_batch_id:
            exit_with_error(log, "file batch id should be provided!")
        log.warn(f"file_batch_id: {file_batch_id}")

        file_source = os.environ.get("FILE_SOURCE", config.get("file_source", ""))
        if not file_source:
            exit_with_error(log, "file source should be provided!")
        log.warn(f"file_source: {file_source}")

        landing_path = os.environ.get("LANDING_PATH", config.get("landing_path", ""))
        if not landing_path:
            exit_with_error(log, "landing path should be provided!")
        log.warn(f"landing_path: {landing_path}")

        pipeline_data_key = os.environ.get("PIPELINE_DATA_KEY", config.get("pipeline_data_key", ""))
        if not pipeline_data_key:
            exit_with_error(log, "pipeline data key should be provided!")

        if file_source:
            fhirbundle_landing_path = os.path.join(landing_path, file_source)
        else:
            fhirbundle_landing_path = landing_path

        log.warn(f"fhirbundle_landing_path: {fhirbundle_landing_path}")

        file_tenant = os.environ.get("FILE_TENANT", config.get("file_tenant", ""))
        log.warn(f"file_tenant: {file_tenant}")

        resource_type = os.environ.get("RESOURCE_TYPE", config.get("resource_type", ""))
        log.warn(f"resource_type: {resource_type}")

        src_file_name = os.environ.get("SRC_FILE_NAME", config.get("src_file_name", ""))
        log.warn(f"src_file_name: {src_file_name}")

        src_organization_id = os.environ.get("SRC_ORGANIZATION_ID", config.get("src_organization_id", ""))
        log.warn(f"src_organization_id: {src_organization_id}")

        # Change FHIRBUNDLE to FHIRBUNDLE_BULK
        fhirbundle_landing_path = fhirbundle_landing_path.replace("FHIRBUNDLE", "FHIRBUNDLE_BULK")

        # Construct fhir bundle temp path
        fhir_temp_path = fhirbundle_landing_path.replace("landing", "temporary")
        fhir_bundle_temp_path = os.path.join(fhir_temp_path, file_batch_id)

        # construct metadata
        metadata = {
            "file_tenant": file_tenant,
            "file_source": file_source,
            "resource_type": resource_type,
            "file_batch_id": file_batch_id,
            "src_file_name": src_file_name,
            "src_organization_id": src_organization_id,
        }

        # add storage context in spark session
        spark = add_storage_context(spark, [delta_table_location])

        # load the records from delta table location
        log.warn("load records from delta table location")
        src_df = (
            spark.read.format("delta")
            .load(delta_table_location)
            .filter(f.col("batch_id") == file_batch_id)
            .drop(
                "batch_id",
                "source_system",
                "file_name",
                "status",
                "created_user",
                "created_ts",
                "updated_user",
                "updated_ts",
            )
            .fillna("")
        )

        if src_df.isEmpty():
            log.warn(f"Spark job {app_name} completed successfully, There is lack of adequate rows for processing.")
            sys.exit(0)

        # transformation
        claim_source_id = f.concat(
            f.col("member_id"),
            f.when(f.col("date_of_service") != "", f.concat(f.lit("_"), f.col("date_of_service"))).otherwise(f.lit("")),
            f.when(f.col("claim_id") != "", f.concat(f.lit("_"), f.col("claim_id"))).otherwise(f.lit("")),
        )

        # apply transformation
        src_df = (
            src_df.withColumn("patient_internal_id", f.expr("uuid()"))
            .withColumn(
                "practitioner_internal_id",
                f.when(f.col("provider_id") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn("encounter_internal_id", f.expr("uuid()"))
            .withColumn("claim_internal_id", f.expr("uuid()"))
            .withColumn("claim_response_internal_id", f.expr("uuid()"))
            .withColumn("bundle_id", f.col("row_id"))
            .withColumn("claim_source_id", claim_source_id)
            .withColumn("file_tenant", f.lit(file_tenant))
            .withColumn("file_source", f.lit(file_source))
            .withColumn("resource_type", f.lit(resource_type))
            .withColumn("file_batch_id", f.lit(file_batch_id))
            .withColumn("src_file_name", f.lit(src_file_name))
            .withColumn("src_organization_id", f.lit(src_organization_id))
        )

        src_df.persist()

        # consolidate diagnosis
        diag_df = (
            src_df.withColumn(
                "diagnosis_code_list",
                f.array(
                    f.col("principal_icd_diagnosis"),
                    f.col("icd_diagnosis_2"),
                    f.col("icd_diagnosis_3"),
                    f.col("icd_diagnosis_4"),
                    f.col("icd_diagnosis_5"),
                    f.col("icd_diagnosis_6"),
                    f.col("icd_diagnosis_7"),
                    f.col("icd_diagnosis_8"),
                    f.col("icd_diagnosis_9"),
                    f.col("icd_diagnosis_10"),
                ),
            )
            .withColumn("diagnosis_code_list", f.array_distinct(f.expr("filter(diagnosis_code_list, x -> x != '')")))
            .withColumn("diagnosis_code", f.explode(f.col("diagnosis_code_list")))
            .select(["row_id", "diagnosis_code", "date_of_service", "icd_identifier"])
        )
        diag_window_spec = w.partitionBy("row_id").orderBy("row_id")
        diag_seq_df = diag_df.withColumn("sequence", f.row_number().over(diag_window_spec))
        diag_seq_df.persist()

        diag_seq_final_df = (
            diag_seq_df.groupBy("row_id")
            .agg(f.collect_list("sequence").alias("diag_sequences"))
            .select(["row_id", "diag_sequences"])
        )
        diag_seq_final_df.persist()

        diag_struct_df = (
            diag_seq_df.withColumn("condition_internal_id", f.expr("uuid()"))
            .withColumn(
                "diagnosis_struct",
                f.struct(
                    f.col("sequence"),
                    f.col("condition_internal_id"),
                    f.col("diagnosis_code").alias("code"),
                    f.col("date_of_service").alias("onset_date_time"),
                    f.col("icd_identifier"),
                ),
            )
            .select(["row_id", "diagnosis_struct"])
        )
        diag_struct_final_df = diag_struct_df.groupBy("row_id").agg(
            f.collect_list("diagnosis_struct").alias("diagnosis_structs")
        )
        diag_struct_final_df.persist()

        # consolidate procedure
        proc_df = (
            src_df.withColumn(
                "procedure_code_list",
                f.array(
                    f.col("principal_icd_procedure"),
                    f.col("icd_procedure_2"),
                    f.col("icd_procedure_3"),
                    f.col("icd_procedure_4"),
                    f.col("icd_procedure_5"),
                    f.col("icd_procedure_6"),
                ),
            )
            .withColumn("procedure_code_list", f.array_distinct(f.expr("filter(procedure_code_list, x -> x != '')")))
            .withColumn("procedure_code", f.explode(f.col("procedure_code_list")))
            .select(["row_id", "procedure_code", "date_of_service", "icd_identifier"])
        )
        proc_window_spec = w.partitionBy("row_id").orderBy("row_id")
        proc_seq_df = proc_df.withColumn("sequence", f.row_number().over(proc_window_spec))
        proc_seq_df.persist()

        proc_seq_final_df = (
            proc_seq_df.groupBy("row_id")
            .agg(f.collect_list("sequence").alias("proc_sequences"))
            .select(["row_id", "proc_sequences"])
        )
        proc_seq_final_df.persist()

        proc_struct_df = (
            proc_seq_df.withColumn("procedure_internal_id", f.expr("uuid()"))
            .withColumn(
                "procedure_struct",
                f.struct(
                    f.col("sequence"),
                    f.col("procedure_internal_id"),
                    f.col("procedure_code").alias("code"),
                    f.col("date_of_service").alias("performed_date_time"),
                    f.col("icd_identifier"),
                ),
            )
            .select(["row_id", "procedure_struct"])
        )
        proc_struct_final_df = proc_struct_df.groupBy("row_id").agg(
            f.collect_list("procedure_struct").alias("procedure_structs")
        )
        proc_struct_final_df.persist()

        # join diagnosis, procedure sequences and structs into source dataframe
        data_df = src_df.join(
            diag_seq_final_df,
            src_df.row_id == diag_seq_final_df.row_id,
            how="left",
        ).select(src_df["*"], diag_seq_final_df["diag_sequences"])
        data_df = data_df.join(
            proc_seq_final_df,
            data_df.row_id == proc_seq_final_df.row_id,
            how="left",
        ).select(data_df["*"], proc_seq_final_df["proc_sequences"])
        data_df = data_df.join(
            diag_struct_final_df,
            data_df.row_id == diag_struct_final_df.row_id,
            how="left",
        ).select(data_df["*"], diag_struct_final_df["diagnosis_structs"])
        data_df = data_df.join(
            proc_struct_final_df,
            data_df.row_id == proc_struct_final_df.row_id,
            how="left",
        ).select(data_df["*"], proc_struct_final_df["procedure_structs"])
        data_df.persist()

        src_df.unpersist()
        diag_seq_df.unpersist()
        diag_seq_final_df.unpersist()
        diag_struct_final_df.unpersist()
        proc_seq_df.unpersist()
        proc_seq_final_df.unpersist()
        proc_struct_final_df.unpersist()

        # fhir mapper
        data_df = fhir_mapper_df(data_df)

        # processing row wise operation
        transformer = FHIRTransformer()
        data_rdd = data_df.rdd.map(lambda row: render_resources(row, transformer))
        resources_df = spark.createDataFrame(data_rdd)
        resources_df.write.mode("overwrite").text(fhir_bundle_temp_path)

        upload_bundle_files(
            fhir_bundle_temp_path=fhir_bundle_temp_path,
            landing_path=fhirbundle_landing_path,
            metadata=metadata,
            enc_data_key=pipeline_data_key,
        )

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"patient from file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
