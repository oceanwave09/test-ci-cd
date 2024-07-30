import json
import os

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.enums import ResourceType
from utils.transformation import parse_boolean, parse_date, parse_date_time, update_gender
from utils.utils import exit_with_error, load_config, prepare_fhir_bundle

PAT_JINJA_TEMPLATE = "patient.j2"
PRC_JINJA_TEMPLATE = "practitioner.j2"
COV_JINJA_TEMPLATE = "coverage.j2"
PRC_ROLE_JINJA_TEMPLATE = "practitioner_role.j2"
ORG_JINJA_TEMPLATE = "organization.j2"


def _transform_patient(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("patient_internal_id", "")
    data_dict["practitioners"] = [{"id": row_dict.get("patient_practitioner_id", "")}]
    data_dict["organization_id"] = row_dict.get("patient_organization_id", "")
    member_id = row_dict.get("patient_member_id", "")
    subscrier_id = row_dict.get("patient_subscriber_id", "")
    member_suffix = row_dict.get("patient_member_id_suffix", "")
    if not member_id:
        member_id = subscrier_id + member_suffix
    data_dict["member_id"] = member_id
    data_dict["member_system"] = "http://healthec.com/identifier/member_id"
    data_dict["mbi"] = row_dict.get("patient_mbi", "")
    data_dict["medicaid_id"] = row_dict.get("patient_medicaid_id", "")
    data_dict["subscriber_system"] = "http://healthec.com/identifier/subscriber_id"
    data_dict["subscriber_id"] = subscrier_id
    data_dict["ssn"] = row_dict.get("patient_ssn", "")
    data_dict["source_id"] = row_dict.get("patient_source_id", "")
    data_dict["lastname"] = row_dict.get("patient_lastname", "")
    data_dict["firstname"] = row_dict.get("patient_firstname", "")
    data_dict["middleinitials"] = row_dict.get("patient_middle_initial", "")
    data_dict["prefix"] = row_dict.get("patient_name_prefix", "")
    data_dict["suffix"] = row_dict.get("patient_name_suffix", "")
    data_dict["dob"] = parse_date(row_dict.get("patient_dob", ""))
    data_dict["gender"] = update_gender(row_dict.get("patient_gender", ""))
    data_dict["street_address_1"] = row_dict.get("patient_street_address_1", "")
    data_dict["street_address_2"] = row_dict.get("patient_street_address_2", "")
    data_dict["city"] = row_dict.get("patient_city", "")
    data_dict["district_code"] = row_dict.get("patient_district", "")
    data_dict["state"] = row_dict.get("patient_state", "")
    data_dict["zip"] = row_dict.get("patient_zip", "")
    data_dict["country"] = row_dict.get("patient_country", "")
    data_dict["phone_home"] = row_dict.get("patient_phone_home", "")
    data_dict["phone_work"] = row_dict.get("patient_phone_work", "")
    data_dict["phone_mobile"] = row_dict.get("patient_phone_mobile", "")
    data_dict["email"] = row_dict.get("patient_email", "")
    data_dict["deceased_boolean"] = parse_boolean(row_dict.get("patient_deceased_boolean", ""))
    data_dict["deceased_date_time"] = parse_date_time(row_dict.get("patient_deceased_date_time", ""))
    data_dict["preferred_language_display"] = row_dict.get("patient_preferred_language_display", "")
    data_dict["preferred_language_text"] = row_dict.get("patient_preferred_language_display", "")
    data_dict["race_code"] = row_dict.get("patient_race_code", "")
    data_dict["race_display"] = row_dict.get("patient_race_display", "")
    data_dict["ethnicity_code"] = row_dict.get("patient_ethnicity_code", "")
    data_dict["ethnicity_display"] = row_dict.get("patient_ethnicity_display", "")
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Patient.value, data_dict)]}
    return Row(**resource_dict)


def _transform_provider(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("practitioner_internal_id", "")
    data_dict["npi"] = row_dict.get("practitioner_npi", "")
    data_dict["source_id"] = row_dict.get("practitioner_source_id", "")
    data_dict["firstname"] = row_dict.get("practitioner_first_name", "")
    data_dict["lastname"] = row_dict.get("practitioner_last_name", "")
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Practitioner.value, data_dict)]}
    return Row(**resource_dict)


def _transform_coverage(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("coverage_internal_id", "")
    data_dict["beneficiary_patient_id"] = row_dict.get("coverage_patient_id", "")
    data_dict["insurer_organization_id"] = row_dict.get("coverage_organization_id", "")
    member_id = row_dict.get("coverage_member_id", "")
    subscrier_id = row_dict.get("coverage_subscriber_id", "")
    member_suffix = row_dict.get("coverage_subscriber_suffix", "")
    if not member_id:
        member_id = subscrier_id + member_suffix
    data_dict["member_id"] = member_id
    data_dict["member_system"] = "http://healthec.com/identifier/member_id"
    data_dict["subscriber_id"] = subscrier_id
    data_dict["subscriber_system"] = "http://healthec.com/identifier/subscriber_id"
    data_dict["relationship_code"] = (
        row_dict.get("coverage_relationship_code", "")
        if row_dict.get("coverage_relationship_code", "")
        else member_suffix
    )
    data_dict["status"] = "active"
    data_dict["kind"] = "insurance"
    data_dict["period_start_date"] = parse_date_time(row_dict.get("coverage_period_start", ""))
    data_dict["period_end_date"] = parse_date_time(row_dict.get("coverage_period_end", ""))
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Coverage.value, data_dict)]}
    return Row(**resource_dict)


# def _transform_provider_organization(row: Row, transformer: FHIRTransformer) -> Row:
#     row_dict = row.asDict(recursive=True)
#     data_dict = {}
#     data_dict["internal_id"] = row_dict.get("organization_internal_id", "")
#     data_dict["tax_id"] = row_dict.get("organization_tax_id", "")
#     data_dict["name"] = row_dict.get("organization_name", "")
#     data_dict["type_code"] = "prov"
#     data_dict["type_display"] = "Healthcare Provider"
#     data_dict["type_system"] = "http://hl7.org/fhir/ValueSet/organization-type"
#     data_dict["type_text"] = "Healthcare Provider"
#     resource_dict = {"resources": [transformer.render_resource(ResourceType.Organization.value, data_dict)]}
#     return Row(**resource_dict)


def _transform_insurer_organization(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("organization_internal_id", "")
    data_dict["source_id"] = row_dict.get("organization_source_id", "")
    data_dict["name"] = row_dict.get("organization_name", "")
    data_dict["type_code"] = "pay"
    data_dict["type_display"] = "Payer"
    data_dict["type_system"] = "http://hl7.org/fhir/ValueSet/organization-type"
    data_dict["type_text"] = "Payer"
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Organization.value, data_dict)]}
    return Row(**resource_dict)


# def _transform_provider_role(row: Row, transformer: FHIRTransformer) -> Row:
#     row_dict = row.asDict(recursive=True)
#     data_dict = {}
#     data_dict["internal_id"] = row_dict.get("practitioner_role_internal_id", "")
#     data_dict["practitioner_id"] = row_dict.get("practitioner_role_practitioner_id", "")
#     data_dict["organization_id"] = row_dict.get("practitioner_role_organization_id", "")
#     data_dict["specialties"] = [{"code": row_dict.get("practitioner_role_specialty_code", "")}]
#     data_dict["role_code"] = row_dict.get("practitioner_role_role_code", "")
#     data_dict["role_display"] = row_dict.get("practitioner_role_role_display", "")
#     data_dict["period_start_date"] = parse_date_time(row_dict.get("practitioner_role_period_start", ""))
#     data_dict["period_end_date"] = parse_date_time(row_dict.get("practitioner_role_period_end", ""))
#     resource_dict = {"resources": [transformer.render_resource(ResourceType.PractitionerRole.value, data_dict)]}
#     return Row(**resource_dict)


def render_patient(spark: SparkSession, transformer: FHIRTransformer, seq: int = 1):
    transformer.load_template(PAT_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        pat_df = (
            df.withColumn(
                "patient_internal_id",
                f.concat_ws("_", f.lit("Patient"), f.col("row_id"), f.lit(seq)),
            )
            .withColumnRenamed("provider_internal_id", "patient_practitioner_id")
            .withColumnRenamed("insurer_internal_id", "patient_organization_id")
            .withColumn("patient_member_id", get_column(df, "member_id"))
            .withColumn("patient_mbi", get_column(df, "member_mbi"))
            .withColumn("patient_medicaid_id", get_column(df, "member_medicaid_id"))
            .withColumn("patient_subscriber_id", get_column(df, "subscriber_id"))
            .withColumn("patient_member_id_suffix", get_column(df, "member_id_suffix"))
            .withColumn("patient_alternate_member_id", get_column(df, "alternate_member_id"))
            .withColumn("patient_ssn", get_column(df, "member_ssn"))
            .withColumn("patient_source_id", get_column(df, "member_internal_id"))
            .withColumn("patient_lastname", get_column(df, "member_last_name"))
            .withColumn("patient_firstname", get_column(df, "member_first_name"))
            .withColumn("patient_middle_initial", get_column(df, "member_middle_initial"))
            .withColumn("patient_name_suffix", get_column(df, "member_name_suffix"))
            .withColumn("patient_name_prefix", get_column(df, "member_name_prefix"))
            .withColumn("patient_dob", get_column(df, "member_dob"))
            .withColumn("patient_gender", get_column(df, "member_gender"))
            .withColumn("patient_street_address_1", get_column(df, "member_address_line_1"))
            .withColumn("patient_street_address_2", get_column(df, "member_address_line_2"))
            .withColumn("patient_city", get_column(df, "member_city"))
            .withColumn("patient_district", get_column(df, "member_county"))
            .withColumn("patient_state", get_column(df, "member_state"))
            .withColumn("patient_zip", get_column(df, "member_zip"))
            .withColumn("patient_country", get_column(df, "member_country"))
            .withColumn("patient_phone_home", get_column(df, "member_phone_home"))
            .withColumn("patient_phone_work", get_column(df, "member_phone_work"))
            .withColumn("patient_phone_mobile", get_column(df, "member_phone_mobile"))
            .withColumn("patient_email", get_column(df, "member_email"))
            .withColumn("patient_deceased_boolean", get_column(df, "member_deceased_flag"))
            .withColumn("patient_deceased_date_time", get_column(df, "member_deceased_date"))
            .withColumn("patient_preferred_language_display", get_column(df, "member_primary_language"))
            .withColumn("patient_race_code", get_column(df, "member_race_code"))
            .withColumn("patient_race_display", get_column(df, "member_race_desc"))
            .withColumn("patient_ethnicity_code", get_column(df, "member_ethnicity_code"))
            .withColumn("patient_ethnicity_display", get_column(df, "member_ethnicity_desc"))
            .select(df.colRegex("`patient_.*`"))
        )
        pat_rdd = pat_df.rdd.map(lambda row: _transform_patient(row, transformer))
        pat_df = spark.createDataFrame(pat_rdd)
        return pat_df

    return inner


def render_provider(spark: SparkSession, transformer: FHIRTransformer, seq: int = 1, org_seq: int = 1):
    transformer.load_template(PRC_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        prov_df = (
            df.withColumnRenamed("provider_internal_id", "practitioner_internal_id")
            .withColumn("practitioner_npi", get_column(df, "physician_npi"))
            .withColumn("practitioner_source_id", get_column(df, "physician_internal_id"))
            .withColumn("practitioner_first_name", get_column(df, "physician_first_name"))
            .withColumn("practitioner_last_name", get_column(df, "physician_last_name"))
            .select(df.colRegex("`practitioner_.*`"))
        )
        prov_rdd = prov_df.rdd.map(lambda row: _transform_provider(row, transformer))
        prov_df = spark.createDataFrame(prov_rdd)
        return prov_df

    return inner


def render_coverage(
    spark: SparkSession, transformer: FHIRTransformer, seq: int = 1, pat_seq: int = 1, insurer_seq: int = 1
):
    transformer.load_template(COV_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        cov_df = (
            df.withColumn(
                "coverage_internal_id",
                f.concat_ws("_", f.lit("Coverage"), f.col("row_id"), f.lit(seq)),
            )
            .withColumn(
                "coverage_patient_id",
                f.concat_ws("_", f.lit("Patient"), f.col("row_id"), f.lit(pat_seq)),
            )
            .withColumnRenamed("insurer_internal_id", "coverage_organization_id")
            .withColumn("coverage_member_id", get_column(df, "member_id"))
            .withColumn("coverage_subscriber_id", get_column(df, "subscriber_id"))
            .withColumn("coverage_subscriber_suffix", get_column(df, "member_id_suffix"))
            .withColumn("coverage_relationship_code", get_column(df, "member_relationship_code"))
            .withColumn("coverage_period_start", get_column(df, "insurance_effective_date"))
            .withColumn("coverage_period_end", get_column(df, "insurance_end_date"))
            .select(df.colRegex("`coverage_.*`"))
        )
        cov_rdd = cov_df.rdd.map(lambda row: _transform_coverage(row, transformer))
        cov_df = spark.createDataFrame(cov_rdd)
        return cov_df

    return inner


def render_insurer_organization(spark: SparkSession, transformer: FHIRTransformer, seq: int = 1):
    transformer.load_template(ORG_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        org_df = (
            df.withColumnRenamed("insurer_internal_id", "organization_internal_id")
            .withColumn("organization_name", get_column(df, "insurance_company_name"))
            .withColumn("organization_source_id", get_column(df, "insurance_company_id"))
            .select(df.colRegex("`organization_.*`"))
        )
        org_rdd = org_df.rdd.map(lambda row: _transform_insurer_organization(row, transformer))
        org_df = spark.createDataFrame(org_rdd)
        return org_df

    return inner


# def render_provider_organization(spark: SparkSession, transformer: FHIRTransformer):
#     transformer.load_template(ORG_JINJA_TEMPLATE)

#     def inner(df: DataFrame):
#         org_df = (
#             df.withColumnRenamed("practice_internal_id", "organization_internal_id")
#             .withColumn("organization_tax_id", get_column(df, "provider_org_tin"))
#             .withColumn("organization_name", get_column(df, "provider_org_name"))
#             .select(df.colRegex("`organization_.*`"))
#         )
#         org_rdd = org_df.rdd.map(lambda row: _transform_provider_organization(row, transformer))
#         org_df = spark.createDataFrame(org_rdd)
#         return org_df

#     return inner


# def render_provider_role(spark: SparkSession, transformer: FHIRTransformer):
#     transformer.load_template(PRC_ROLE_JINJA_TEMPLATE)

#     def inner(df: DataFrame):
#         prov_role_df = (
#             df.withColumnRenamed("provider_role_internal_id", "practitioner_role_internal_id")
#             .withColumnRenamed("provider_internal_id", "practitioner_role_practitioner_id")
#             .withColumnRenamed("practice_internal_id", "practitioner_role_organization_id")
#             .withColumn("practitioner_role_specialty_code", get_column(df, "physician_taxonomy_code"))
#             .withColumn("practitioner_role_role_code", get_column(df, "physician_specialty_code"))
#             .withColumn("practitioner_role_role_display", get_column(df, "physician_specialty_description"))
#             .withColumn("practitioner_role_period_start", get_column(df, "physician_effective_date"))
#             .withColumn("practitioner_role_period_end", get_column(df, "physician_termination_date"))
#             .select(df.colRegex("`practitioner_role_.*`"))
#         )
#         prov_role_rdd = prov_role_df.rdd.map(lambda row: _transform_provider_role(row, transformer))
#         prov_role_df = spark.createDataFrame(prov_role_rdd)
#         return prov_role_df

#     return inner


def to_fhir(spark: SparkSession, fhirbundle_landing_path: str, metadata: str = ""):
    def inner(df: DataFrame):
        # initialize fhir transformer
        transformer = FHIRTransformer()

        # prepares fhir:Organization(insurer)
        insurer_cols = ["insurance_company_id", "insurance_company_name"]
        src_insurer_df = (
            df.filter((f.col("insurance_company_id") != ""))
            .select(insurer_cols)
            .drop_duplicates()
            .withColumn("insurer_internal_id", f.concat_ws("_", f.lit("Organization"), f.expr("uuid()"), f.lit(1)))
        )
        resources_df = src_insurer_df.transform(render_insurer_organization(spark, transformer))
        df = df.join(src_insurer_df, on=insurer_cols, how="left").fillna("")

        # prepares fhir:Organization(provider)
        # uniq_practice_df = (
        #     df.filter((f.col("provider_org_tin") != "") | (f.col("provider_org_name") != ""))
        #     .select(["provider_org_tin", "provider_org_name"])
        #     .drop_duplicates(["provider_org_tin", "provider_org_name"])
        #     .withColumn("practice_internal_id", f.concat_ws("_", f.lit("Organization"), f.expr("uuid()"), f.lit(2)))
        # )
        # if len(uniq_practice_df.head(1)) > 0:
        #     practice_df = uniq_practice_df.transform(render_provider_organization(spark, transformer))
        #     resources_df = resources_df.union(practice_df)
        #     df = df.join(uniq_practice_df, on=["provider_org_tin", "provider_org_name"], how="left").fillna("")
        # else:
        #     df = df.withColumn("practice_internal_id", get_column(df, "practice_internal_id")).fillna("")

        # prepares fhir:Practitioner
        provider_cols = ["physician_npi", "physician_internal_id", "physician_first_name", "physician_last_name"]
        src_provider_df = (
            df.filter(f.col("physician_npi") != "")
            .select(provider_cols)
            .drop_duplicates()
            .withColumn("provider_internal_id", f.concat_ws("_", f.lit("Practitioner"), f.expr("uuid()"), f.lit(1)))
        )
        if not src_provider_df.isEmpty():
            prov_df = src_provider_df.transform(render_provider(spark, transformer))
            resources_df = resources_df.union(prov_df)
            df = df.join(src_provider_df, on=provider_cols, how="left").fillna("")

        # # prepares fhir:PractitionerRole
        # provider_role_cols = [
        #     "provider_internal_id",
        #     "practice_internal_id",
        #     "physician_taxonomy_code",
        #     "physician_specialty_code",
        #     "physician_specialty_description",
        #     "physician_effective_date",
        #     "physician_termination_date",
        # ]
        # uniq_provider_role_df = (
        #     df.filter((f.col("provider_internal_id") != "") & (f.col("practice_internal_id") != ""))
        #     .select(*provider_role_cols)
        #     .drop_duplicates(*provider_role_cols)
        #     .withColumn(
        #         "provider_role_internal_id", f.concat_ws("_", f.lit("PractitionerRole"), f.expr("uuid()"), f.lit(1))
        #     )
        # )
        # if len(uniq_provider_role_df.head(1)) > 0:
        #     resources_df = uniq_provider_role_df.transform(render_provider_role(spark, transformer))
        #     drop_cols = (
        #         "physician_taxonomy_code",
        #         "physician_specialty_code",
        #         "physician_specialty_description",
        #         "physician_effective_date",
        #         "physician_termination_date",
        #     )
        #     df = df.join(
        #         uniq_provider_role_df.drop(*drop_cols), on=["provider_internal_id", "practice_internal_id"], how="left"
        #     ).fillna("")

        # prepares fhir:Patient
        pat_df = df.transform(render_patient(spark, transformer))
        resources_df = resources_df.union(pat_df)

        # prepares fhir:Coverage
        cov_df = df.transform(render_coverage(spark, transformer))
        resources_df = resources_df.union(cov_df)

        # collect all resources into a list and create fhir bundle for each file
        prepare_bundle_udf = f.udf(prepare_fhir_bundle)
        resources_df = resources_df.withColumn("resources", f.explode("resources")).agg(
            f.collect_list("resources").alias("resources")
        )
        resources_df = resources_df.withColumn("bundle_id", f.expr("uuid()"))
        fhir_df = resources_df.withColumn(
            "bundle",
            prepare_bundle_udf(
                f.col("bundle_id"),
                f.col("resources"),
                f.lit(fhirbundle_landing_path),
                f.lit(metadata),
            ),
        ).drop("resources")

        return fhir_df

    return inner


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="beneficiary_stage_to_fhirbundle_job",
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
        delta_schema_location = os.environ.get("DELTA_SCHEMA_LOCATION", config.get("delta_schema_location", ""))
        if not delta_schema_location:
            exit_with_error(log, "delta schema location should be provided!")

        delta_table_name = os.environ.get("DELTA_TABLE_NAME", config.get("delta_table_name", "beneficiary"))
        if not delta_table_name:
            exit_with_error(log, "delta table location should be provided!")

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

        fhirbundle_landing_path = os.path.join(landing_path, file_source)
        log.warn(f"fhirbundle_landing_path: {fhirbundle_landing_path}")

        file_tenant = os.environ.get("FILE_TENANT", config.get("file_tenant", ""))
        log.warn(f"file_tenant: {file_tenant}")

        resource_type = os.environ.get("RESOURCE_TYPE", config.get("resource_type", ""))
        log.warn(f"resource_type: {resource_type}")

        src_file_name = os.environ.get("SRC_FILE_NAME", config.get("src_file_name", ""))
        log.warn(f"src_file_name: {src_file_name}")

        src_organization_id = os.environ.get("SRC_ORGANIZATION_ID", config.get("src_organization_id", ""))
        log.warn(f"src_organization_id: {src_organization_id}")

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

        pay_df = data_df.filter(f.col("insurance_company_id") == "")
        if not pay_df.isEmpty():
            exit_with_error(log, "insurance company id must be a not-null value.")
        pat_df = data_df.filter((f.col("member_id") == "") | (f.col("member_dob") == ""))
        if not pat_df.isEmpty():
            exit_with_error(log, "member dob and member id must be not-null values.")

        # transform into fhir bundle and write into target location
        fhir_df = data_df.transform(to_fhir(spark, fhirbundle_landing_path, json.dumps(metadata)))

        fhir_df.head(1)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"patient from file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
