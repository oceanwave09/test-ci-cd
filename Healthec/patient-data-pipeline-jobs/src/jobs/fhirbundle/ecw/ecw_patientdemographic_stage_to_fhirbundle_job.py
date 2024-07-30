import json
import os
import sys
from string import Template

import click
from fhirclient.resources.organization import Organization
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, start_spark
from utils.api_client import match_core_entity
from utils.enums import ResourceType
from utils.transformation import transform_date, transform_date_time
from utils.utils import exit_with_error, load_config, upload_bundle_files
from utils.constants import PATIENT_MRN_SYSTEM

PAT_JINJA_TEMPLATE = "patient.j2"
PRC_JINJA_TEMPLATE = "practitioner.j2"
COV_JINJA_TEMPLATE = "coverage.j2"
ORG_JINJA_TEMPLATE = "organization.j2"


def _get_organization_match_attributes(value: str) -> str:
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
                    "system": "http://healthec.com/identifier/external_id",
                    "value": "$external_id"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(external_id=value)


def _transform_practice(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)
    # prepare matching attribute
    row_dict["attributes"] = _get_organization_match_attributes(row_dict.get("apuid")) if row_dict.get("apuid") else ""
    return Row(**row_dict)


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


def fhir_mapper_df(df: DataFrame) -> DataFrame:
    df = df.select(
        "bundle_id",
        f.map_from_arrays(
            f.array(f.lit("practitioners")),
            f.array(f.array(f.map_from_entries(f.array(f.struct(f.lit("id"), f.col("practitioner_internal_id")))))),
        ).alias("patient_practitioner_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("mrn"),
                f.lit("organization_id"),
                f.lit("assigner_organization_id"),
                f.lit("mrn_system"),
                f.lit("firstname"),
                f.lit("lastname"),
                f.lit("middleinitials"),
                f.lit("suffix"),
                f.lit("prefix"),
                f.lit("dob"),
                f.lit("gender"),
                f.lit("street_address_1"),
                f.lit("street_address_2"),
                f.lit("city"),
                f.lit("state"),
                f.lit("zip"),
                f.lit("marital_status_display"),
                f.lit("marital_status_system"),
                f.lit("marital_status_code"),
                f.lit("marital_status_text"),
                f.lit("race_code"),
                f.lit("preferred_language_display"),
                f.lit("preferred_language_text"),
                f.lit("ethnicity_code"),
                f.lit("email"),
                f.lit("phone_home"),
                f.lit("phone_mobile"),
                f.lit("deceased_indication"),
                f.lit("deceased_date_time"),
            ),
            f.array(
                f.col("patient_internal_id"),
                f.col("patient_id"),
                f.col("organization_id"),
                f.col("organization_id"),
                f.lit(PATIENT_MRN_SYSTEM),
                f.col("first_name"),
                f.col("last_name"),
                f.col("middle_name"),
                f.col("name_suffix"),
                f.col("name_prefix"),
                f.col("patient_dob"),
                f.col("patient_gender"),
                f.col("address_line_1"),
                f.col("address_line_2"),
                f.col("city"),
                f.col("state"),
                f.col("zip"),
                f.col("marital_status_display"),
                f.lit("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus"),
                f.col("marital_status_code"),
                f.col("marital_status_display"),
                f.col("race"),
                f.col("language"),
                f.col("language"),
                f.col("ethnic_group"),
                f.col("primary_email"),
                f.col("primary_phone_number"),
                f.col("secondary_phone_number"),
                f.col("patient_death_indication_status"),
                f.col("patient_deceased_date_time"),
            ),
        ).alias("patient_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_id"),
                f.lit("source_file_name"),
                f.lit("assigner_organization_id"),
                f.lit("lastname"),
                f.lit("npi"),
                f.lit("firstname"),
                f.lit("middleinitials"),
                f.lit("suffix"),
                f.lit("prefix"),
            ),
            f.array(
                f.col("practitioner_internal_id"),
                f.col("primary_care_provider_id"),
                f.col("file_format"),
                f.col("organization_id"),
                f.col("primary_care_provider_last_name"),
                f.col("provider_npi"),
                f.col("primary_care_provider_first_name"),
                f.col("primary_care_provider_middle_initial"),
                f.col("primary_care_suffix"),
                f.col("primary_care_prefix"),
            ),
        ).alias("practitioner_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_id"),
                f.lit("source_file_name"),
                f.lit("assigner_organization_id"),
                f.lit("name"),
                f.lit("type_code"),
                f.lit("type_display"),
                f.lit("type_system"),
                f.lit("type_text"),
            ),
            f.array(
                f.col("primary_payer_internal_id"),
                f.col("primary_insurance_payer_code"),
                f.col("file_format"),
                f.col("organization_id"),
                f.col("primary_insurance_payor_name"),
                f.lit("pay"),
                f.lit("Payer"),
                f.lit("http://hl7.org/fhir/ValueSet/organization-type"),
                f.lit("Payer"),
            ),
        ).alias("primary_payer_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_id"),
                f.lit("source_file_name"),
                f.lit("assigner_organization_id"),
                f.lit("name"),
                f.lit("type_code"),
                f.lit("type_display"),
                f.lit("type_system"),
                f.lit("type_text"),
            ),
            f.array(
                f.col("secondary_payer_internal_id"),
                f.col("secondary_insurance_payer_code"),
                f.col("file_format"),
                f.col("organization_id"),
                f.col("secondary_insurance_payor_name"),
                f.lit("pay"),
                f.lit("Payer"),
                f.lit("http://hl7.org/fhir/ValueSet/organization-type"),
                f.lit("Payer"),
            ),
        ).alias("secondary_payer_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_id"),
                f.lit("source_file_name"),
                f.lit("assigner_organization_id"),
                f.lit("name"),
                f.lit("type_code"),
                f.lit("type_display"),
                f.lit("type_system"),
                f.lit("type_text"),
            ),
            f.array(
                f.col("tertiary_payer_internal_id"),
                f.col("tertiary_insurance_payer_code"),
                f.col("file_format"),
                f.col("organization_id"),
                f.col("tertiary_insurance_payor_name"),
                f.lit("pay"),
                f.lit("Payer"),
                f.lit("http://hl7.org/fhir/ValueSet/organization-type"),
                f.lit("Payer"),
            ),
        ).alias("tertiary_payer_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("beneficiary_patient_id"),
                f.lit("source_id"),
                f.lit("source_file_name"),
                f.lit("assigner_organization_id"),
                f.lit("subscriber_id"),
                f.lit("medicaid_id"),
                f.lit("insurer_organization_id"),
                f.lit("period_start_date"),
                f.lit("period_end_date"),
                f.lit("status"),
                f.lit("kind"),
            ),
            f.array(
                f.col("primary_coverage_internal_id"),
                f.col("patient_internal_id"),
                f.col("primary_insurance_id"),
                f.col("file_format"),
                f.col("organization_id"),
                f.col("primary_insurance_subscriber_no"),
                f.lit("primary_insurance_medicaid_id"),
                f.col("primary_payer_internal_id"),
                f.col("primary_coverage_period_start"),
                f.col("primary_coverage_period_end"),
                f.lit("active"),
                f.lit("insurance"),
            ),
        ).alias("primary_coverage_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("beneficiary_patient_id"),
                f.lit("source_id"),
                f.lit("source_file_name"),
                f.lit("assigner_organization_id"),
                f.lit("subscriber_id"),
                f.lit("medicaid_id"),
                f.lit("insurer_organization_id"),
                f.lit("period_start_date"),
                f.lit("period_end_date"),
                f.lit("status"),
                f.lit("kind"),
            ),
            f.array(
                f.col("secondary_coverage_internal_id"),
                f.col("patient_internal_id"),
                f.col("secondary_insurance_id"),
                f.col("file_format"),
                f.col("organization_id"),
                f.col("secondary_insurance_subscriber_no"),
                f.lit("secondary_insurance_medicaid_id"),
                f.col("secondary_payer_internal_id"),
                f.col("secondary_coverage_period_start"),
                f.col("secondary_coverage_period_end"),
                f.lit("active"),
                f.lit("insurance"),
            ),
        ).alias("secondary_coverage_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("beneficiary_patient_id"),
                f.lit("source_id"),
                f.lit("file_format"),
                f.lit("assigner_organization_id"),
                f.lit("subscriber_id"),
                f.lit("medicaid_id"),
                f.lit("insurer_organization_id"),
                f.lit("period_start_date"),
                f.lit("period_end_date"),
                f.lit("status"),
                f.lit("kind"),
            ),
            f.array(
                f.col("tertiary_coverage_internal_id"),
                f.col("patient_internal_id"),
                f.col("tertiary_insurance_id"),
                f.col("file_format"),
                f.col("organization_id"),
                f.col("tertiary_insurance_subscriber_no"),
                f.lit("tertiary_insurance_medicaid_id"),
                f.col("tertiary_payer_internal_id"),
                f.col("tertiary_coverage_period_start"),
                f.col("tertiary_coverage_period_end"),
                f.lit("active"),
                f.lit("insurance"),
            ),
        ).alias("tertiary_coverage_rsc"),
    )
    return df


def render_resources(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    resources = []

    # add practitioner reference to patient resource
    get_practitioner_ref = row_dict.get("patient_practitioner_rsc", {})
    get_patient_resource = row_dict.get("patient_rsc", {})
    get_patient_resource.update(get_practitioner_ref)

    # render resources
    resources.append(
        _transform_resource(
            data_dict=get_patient_resource,
            resource_type=ResourceType.Patient.value,
            template=PAT_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("practitioner_rsc"),
            resource_type=ResourceType.Practitioner.value,
            template=PRC_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )
    if row_dict.get("primary_coverage_rsc", {}).get("subscriber_id"):
        resources.append(
            _transform_resource(
                data_dict=row_dict.get("primary_payer_rsc"),
                resource_type=ResourceType.Organization.value,
                template=ORG_JINJA_TEMPLATE,
                transformer=transformer,
            )
        )
        resources.append(
            _transform_resource(
                data_dict=row_dict.get("primary_coverage_rsc"),
                resource_type=ResourceType.Coverage.value,
                template=COV_JINJA_TEMPLATE,
                transformer=transformer,
            )
        )
    if row_dict.get("secondary_coverage_rsc", {}).get("subscriber_id"):
        resources.append(
            _transform_resource(
                data_dict=row_dict.get("secondary_payer_rsc"),
                resource_type=ResourceType.Organization.value,
                template=ORG_JINJA_TEMPLATE,
                transformer=transformer,
            )
        )
        resources.append(
            _transform_resource(
                data_dict=row_dict.get("secondary_coverage_rsc"),
                resource_type=ResourceType.Coverage.value,
                template=COV_JINJA_TEMPLATE,
                transformer=transformer,
            )
        )
    if row_dict.get("tertiary_coverage_rsc", {}).get("subscriber_id"):
        resources.append(
            _transform_resource(
                data_dict=row_dict.get("tertiary_payer_rsc"),
                resource_type=ResourceType.Organization.value,
                template=ORG_JINJA_TEMPLATE,
                transformer=transformer,
            )
        )
        resources.append(
            _transform_resource(
                data_dict=row_dict.get("tertiary_coverage_rsc"),
                resource_type=ResourceType.Coverage.value,
                template=COV_JINJA_TEMPLATE,
                transformer=transformer,
            )
        )
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
    default="ecw_patientdemographic_stage_to_fhirbundle_job",
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
            config.get("delta_table_name", "ecw_patientdemographic"),
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

        file_format = os.environ.get("FILE_FORMAT", config.get("file_format", ""))
        log.warn(f"file_format: {file_format}")

        # Change FHIRBUNDLE to FHIRBUNDLE_BULK
        fhirbundle_landing_path = fhirbundle_landing_path.replace("FHIRBUNDLE", "FHIRBUNDLE_BULK")

        # Construct fhir bundle temp path
        fhir_temp_path = fhirbundle_landing_path.replace("landing", "temporary")
        fhir_bundle_temp_path = os.path.join(fhir_temp_path, file_batch_id)

        # # construct metadata
        metadata = {
            "file_tenant": file_tenant,
            "file_format": file_format,
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
        data_df = (
            spark.read.format("delta")
            .load(delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .drop(
                "file_batch_id",
                "file_name",
                "file_source_name",
                "file_status",
                "created_user",
                "created_ts",
                "updated_user",
                "updated_ts",
            )
            .fillna("")
        )

        if data_df.isEmpty():
            log.warn(f"Spark job {app_name} completed successfully, There is lack of adequate rows for processing.")
            sys.exit(0)

        # apply decryption on personal details
        data_df = (
            data_df.withColumn(
                "first_name",
                f.expr(f"aes_decrypt(unhex(first_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "middle_name",
                f.expr(f"aes_decrypt(unhex(middle_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "last_name",
                f.expr(f"aes_decrypt(unhex(last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "previous_name",
                f.expr(f"aes_decrypt(unhex(previous_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "date_of_birth",
                f.expr(f"aes_decrypt(unhex(date_of_birth) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "address_line_1",
                f.expr(f"aes_decrypt(unhex(address_line_1) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "address_line_2",
                f.expr(f"aes_decrypt(unhex(address_line_2) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "primary_phone_number",
                f.expr(f"aes_decrypt(unhex(primary_phone_number) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "secondary_phone_number",
                f.expr(f"aes_decrypt(unhex(secondary_phone_number) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "primary_email",
                f.expr(f"aes_decrypt(unhex(primary_email) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_npi",
                f.expr(f"aes_decrypt(unhex(provider_npi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "primary_care_provider_last_name",
                f.expr(
                    f"aes_decrypt(unhex(primary_care_provider_last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')"
                ).cast("string"),
            )
            .withColumn(
                "primary_care_provider_first_name",
                f.expr(
                    f"aes_decrypt(unhex(primary_care_provider_first_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')"
                ).cast("string"),
            )
            .withColumn(
                "primary_care_provider_middle_initial",
                f.expr(
                    f"aes_decrypt(unhex(primary_care_provider_middle_initial) ,'{pipeline_data_key}', 'ECB', 'PKCS')"
                ).cast("string"),
            )
        )

        # validate `apuid`
        val_pract_df = data_df.filter(f.col("apuid") == "")
        if not val_pract_df.isEmpty():
            exit_with_error(log, "apuid must be a not-null value.")
        # validate `patient_id`
        val_pat_df = data_df.filter(f.col("patient_id") == "")
        if not val_pat_df.isEmpty():
            exit_with_error(log, "patient id must be a not-null value.")
        # # validate `provider_npi`
        # val_prov_df = data_df.filter(f.col("provider_npi") == "")
        # if not val_prov_df.isEmpty():
        #     exit_with_error(log, "provider npi must be a not-null value.")

        # register provider service udf
        prov_service_udf = f.udf(lambda attr: match_core_entity(attr, Organization))

        # filter valid apuid by tenant
        pract_df = data_df.select("apuid").drop_duplicates()
        pract_rdd = pract_df.rdd.map(lambda row: _transform_practice(row))
        pract_df = spark.createDataFrame(pract_rdd)

        # match organization by external id
        match_pract_df = (
            pract_df.withColumn("organization_id", prov_service_udf(pract_df["attributes"]))
            .filter(f.col("organization_id").isNotNull())
            .select(["apuid", "organization_id"])
        )
        match_pract_df.persist()

        # select rows having organization match
        data_df = data_df.join(match_pract_df, on="apuid", how="inner")

        if data_df.isEmpty():
            log.warn(f"Spark job {app_name} completed successfully, There is lack of adequate rows for processing.")
            sys.exit(0)

        # transformations
        patient_death_indication_status_col = f.when(
            f.col("patient_death_indication") != "",
            f.when(
                f.upper(f.substring(f.col("patient_death_indication"), 0, 1)).isin(["Y", "T", "1"]), "true"
            ).otherwise(f.lit("false")),
        ).otherwise(f.lit("false"))
        patient_gender_col = f.when(
            f.col("gender") != "",
            f.when(f.upper(f.substring(f.col("gender"), 0, 1)).isin(["M", "1"]), "male")
            .when(f.upper(f.substring(f.col("gender"), 0, 1)).isin(["F", "2"]), "female")
            .when(f.upper(f.substring(f.col("gender"), 0, 1)).isin(["T", "O"]), "other")
            .otherwise(f.lit("unknown")),
        ).otherwise(f.lit("unknown"))
        marital_status_code = f.when(
            f.col("marital_status") != "",
            f.when(f.upper(f.col("marital_status")).isin("UNMARRIED", "SINGLE"), "U")
            .when(f.upper(f.col("marital_status")) == "ANNULLED", "A")
            .when(f.upper(f.col("marital_status")).isin("DOMESTIC PARTNER", "DIVORCED"), "D")
            .when(f.upper(f.col("marital_status")) == "INTERLOCUTORY", "I")
            .when(f.upper(f.col("marital_status")) == "SEPARATED", "S")
            .when(f.upper(f.col("marital_status")) == "MARRIED", "M")
            .when(f.upper(f.col("marital_status")) == "COMMON LAW", "C")
            .when(f.upper(f.col("marital_status")) == "POLYGAMOUS ", "P")
            .when(f.upper(f.col("marital_status")) == "NEVER MARRIED", "N")
            .when(f.upper(f.col("marital_status")) == "WIDOWED", "W")
            .otherwise("UNK"),
        ).otherwise("UNK")

        marital_status_display = f.when(
            (f.col("marital_status") != "") & (f.upper(f.col("marital_status")) == "SINGLE"), "unmarried"
        ).otherwise(f.col("marital_status"))

        # apply transformations
        data_df = (
            data_df.withColumn("patient_internal_id", f.expr("uuid()"))
            .withColumn("practitioner_internal_id", f.expr("uuid()"))
            .withColumn(
                "primary_payer_internal_id",
                f.when(f.col("primary_insurance_subscriber_no") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn(
                "secondary_payer_internal_id",
                f.when(f.col("secondary_insurance_subscriber_no") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn(
                "tertiary_payer_internal_id",
                f.when(f.col("tertiary_insurance_subscriber_no") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn(
                "primary_coverage_internal_id",
                f.when(f.col("primary_insurance_subscriber_no") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn(
                "secondary_coverage_internal_id",
                f.when(f.col("secondary_insurance_subscriber_no") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn(
                "tertiary_coverage_internal_id",
                f.when(f.col("tertiary_insurance_subscriber_no") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn("bundle_id", f.col("row_id"))
            .withColumn("file_tenant", f.lit(file_tenant))
            .withColumn("file_format", f.lit(file_format))
            .withColumn("file_source", f.lit(file_source))
            .withColumn("resource_type", f.lit(resource_type))
            .withColumn("file_batch_id", f.lit(file_batch_id))
            .withColumn("src_file_name", f.lit(src_file_name))
            .withColumn("src_organization_id", f.lit(src_organization_id))
            .withColumn("patient_deceased_date_time", transform_date_time(f.col("patient_death_date")))
            .withColumn("primary_coverage_period_start", transform_date_time(f.col("primary_insurance_start_date")))
            .withColumn("primary_coverage_period_end", transform_date_time(f.col("primary_insurance_end_date")))
            .withColumn("secondary_coverage_period_start", transform_date_time(f.col("secondary_insurance_start_date")))
            .withColumn("secondary_coverage_period_end", transform_date_time(f.col("secondary_insurance_end_date")))
            .withColumn("tertiary_coverage_period_start", transform_date_time(f.col("tertiary_insurance_start_date")))
            .withColumn("tertiary_coverage_period_end", transform_date_time(f.col("tertiary_insurance_end_date")))
            .withColumn("patient_dob", transform_date(f.col("date_of_birth")))
            .withColumn("patient_death_indication_status", patient_death_indication_status_col)
            .withColumn("patient_gender", patient_gender_col)
            .withColumn("marital_status_code", marital_status_code)
            .withColumn("marital_status_display", marital_status_display)
        )
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
            f"file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
