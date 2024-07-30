import json
import os
import sys

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import broadcast

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.enums import ResourceType
from utils.transformation import parse_date, parse_date_time, update_gender, update_race_code
from utils.utils import exit_with_error, load_config, prepare_fhir_bundle

ORG_JINJA_TEMPLATE = "organization.j2"
PRAC_JINJA_TEMPLATE = "practitioner.j2"
PAT_JINJA_TEMPLATE = "patient.j2"
COV_JINJA_TEMPLATE = "coverage.j2"
CON_JINJA_TEMPLATE = "condition.j2"


def _transform_provider(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("organization_internal_id", "")
    data_dict["tax_id"] = row_dict.get("organization_tax_id", "")
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Organization.value, data_dict)]}
    return Row(**resource_dict)


def _transform_practitioner(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("practitioner_internal_id", "")
    data_dict["npi"] = row_dict.get("practitioner_npi", "")
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Practitioner.value, data_dict)]}
    return Row(**resource_dict)


def _transform_patient(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("patient_internal_id", "")
    data_dict["organization_id"] = row_dict.get("patient_organization_id", "")
    if row_dict.get("patient_practitioner_id", ""):
        data_dict["practitioners"] = [{"id": row_dict.get("patient_practitioner_id", "")}]
    data_dict["member_id"] = row_dict.get("patient_mbi", "")
    data_dict["medicare_number"] = row_dict.get("patient_medicare", "")
    data_dict["firstname"] = row_dict.get("patient_first_name", "")
    data_dict["lastname"] = row_dict.get("patient_last_name", "")
    data_dict["gender"] = update_gender(row_dict.get("patient_gender", ""))
    data_dict["dob"] = parse_date(row_dict.get("patient_dob", ""))
    data_dict["deceased_date_time"] = parse_date_time(row_dict.get("patient_deceased_date", ""))
    data_dict["state"] = row_dict.get("patient_state", "")
    data_dict["county_code"] = row_dict.get("patient_district", "")
    data_dict["race_text"] = update_race_code(row_dict.get("patient_race_code", ""))
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Patient.value, data_dict)]}
    return Row(**resource_dict)


def _add_extension(data_dict: dict, ext_name: str, value: str):
    if value is not None and value != "":
        data_dict["extensions"].append({"url": f"https://cms.gov/resources/variables/{ext_name}", "value": str(value)})


def _transform_coverage(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("coverage_internal_id", "")
    data_dict["beneficiary_patient_id"] = row_dict.get("coverage_patient_id", "")
    data_dict["medicare_number"] = row_dict.get("coverage_medicare_number", "")
    data_dict["extensions"] = []
    _add_extension(data_dict, "cba_flag", row_dict.get("coverage_cba_flag"))
    _add_extension(data_dict, "assignment_type", row_dict.get("coverage_assignment_type"))
    _add_extension(data_dict, "assignment_before", row_dict.get("coverage_assignment_before"))
    _add_extension(data_dict, "asg_status", row_dict.get("coverage_asg_status"))
    _add_extension(data_dict, "partd_months", row_dict.get("coverage_partd_months"))
    _add_extension(data_dict, "excluded", row_dict.get("coverage_excluded"))
    _add_extension(data_dict, "deceased_excluded", row_dict.get("coverage_deceased_excluded"))
    _add_extension(data_dict, "missing_id_excluded", row_dict.get("coverage_missing_id_excluded"))
    _add_extension(data_dict, "part_a_b_only_excluded", row_dict.get("coverage_part_a_b_only_excluded"))
    _add_extension(data_dict, "ghp_excluded", row_dict.get("coverage_ghp_excluded"))
    _add_extension(data_dict, "outside_us_excluded", row_dict.get("coverage_outside_us_excluded"))
    _add_extension(data_dict, "other_shared_sav_init", row_dict.get("coverage_other_shared_sav_init"))
    _add_extension(data_dict, "enroll_flag_1", row_dict.get("coverage_enrollflag_1"))
    _add_extension(data_dict, "enroll_flag_2", row_dict.get("coverage_enrollflag_2"))
    _add_extension(data_dict, "enroll_flag_3", row_dict.get("coverage_enrollflag_3"))
    _add_extension(data_dict, "enroll_flag_4", row_dict.get("coverage_enrollflag_4"))
    _add_extension(data_dict, "enroll_flag_5", row_dict.get("coverage_enrollflag_5"))
    _add_extension(data_dict, "enroll_flag_6", row_dict.get("coverage_enrollflag_6"))
    _add_extension(data_dict, "enroll_flag_7", row_dict.get("coverage_enrollflag_7"))
    _add_extension(data_dict, "enroll_flag_8", row_dict.get("coverage_enrollflag_8"))
    _add_extension(data_dict, "enroll_flag_9", row_dict.get("coverage_enrollflag_9"))
    _add_extension(data_dict, "enroll_flag_10", row_dict.get("coverage_enrollflag_10"))
    _add_extension(data_dict, "enroll_flag_11", row_dict.get("coverage_enrollflag_11"))
    _add_extension(data_dict, "enroll_flag_12", row_dict.get("coverage_enrollflag_12"))
    _add_extension(data_dict, "bene_rsk_r_scre_01", row_dict.get("coverage_bene_rsk_r_scre_01"))
    _add_extension(data_dict, "bene_rsk_r_scre_02", row_dict.get("coverage_bene_rsk_r_scre_02"))
    _add_extension(data_dict, "bene_rsk_r_scre_03", row_dict.get("coverage_bene_rsk_r_scre_03"))
    _add_extension(data_dict, "bene_rsk_r_scre_04", row_dict.get("coverage_bene_rsk_r_scre_04"))
    _add_extension(data_dict, "bene_rsk_r_scre_05", row_dict.get("coverage_bene_rsk_r_scre_05"))
    _add_extension(data_dict, "bene_rsk_r_scre_06", row_dict.get("coverage_bene_rsk_r_scre_06"))
    _add_extension(data_dict, "bene_rsk_r_scre_07", row_dict.get("coverage_bene_rsk_r_scre_07"))
    _add_extension(data_dict, "bene_rsk_r_scre_08", row_dict.get("coverage_bene_rsk_r_scre_08"))
    _add_extension(data_dict, "bene_rsk_r_scre_09", row_dict.get("coverage_bene_rsk_r_scre_09"))
    _add_extension(data_dict, "bene_rsk_r_scre_10", row_dict.get("coverage_bene_rsk_r_scre_10"))
    _add_extension(data_dict, "bene_rsk_r_scre_11", row_dict.get("coverage_bene_rsk_r_scre_11"))
    _add_extension(data_dict, "bene_rsk_r_scre_12", row_dict.get("coverage_bene_rsk_r_scre_12"))
    _add_extension(data_dict, "esrd_score", row_dict.get("coverage_esrd_score"))
    _add_extension(data_dict, "dis_score", row_dict.get("coverage_dis_score"))
    _add_extension(data_dict, "agdu_score", row_dict.get("coverage_agdu_score"))
    _add_extension(data_dict, "agnd_score", row_dict.get("coverage_agnd_score"))
    _add_extension(data_dict, "dem_esrd_score", row_dict.get("coverage_dem_esrd_score"))
    _add_extension(data_dict, "dem_dis_score", row_dict.get("coverage_dem_dis_score"))
    _add_extension(data_dict, "dem_agdu_score", row_dict.get("coverage_dem_agdu_score"))
    _add_extension(data_dict, "dem_agnd_score", row_dict.get("coverage_dem_agnd_score"))
    _add_extension(data_dict, "new_enrollee", row_dict.get("coverage_new_enrollee"))
    _add_extension(data_dict, "lti_status", row_dict.get("coverage_lti_status"))
    _add_extension(data_dict, "va_selection_only", row_dict.get("coverage_va_selection_only"))
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Coverage.value, data_dict)]}
    return Row(**resource_dict)


def _transform_condition(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("condition_internal_id", "")
    if row_dict.get("condition_practitioner_id", ""):
        data_dict["practitioner_id"] = row_dict.get("condition_practitioner_id", "")
    data_dict["patient_id"] = row_dict.get("condition_patient_id", "")
    data_dict["code_system"] = "http://terminology.hl7.org/CodeSystem/cmshcc"
    data_dict["code"] = row_dict.get("condition_hcc_code", "")
    data_dict["code_display"] = row_dict.get("condition_hcc_code_display", "")
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Condition.value, data_dict)]}
    return Row(**resource_dict)


def render_provider(spark: SparkSession, transformer: FHIRTransformer, seq: int = 1):
    transformer.load_template(ORG_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        prov_df = (
            df.withColumn(
                "organization_internal_id",
                f.concat_ws("_", f.lit("Organization"), f.col("row_id"), f.lit(seq)),
            )
            .withColumn("organization_tax_id", get_column(df, "master_id"))
            .select(df.colRegex("`organization_.*`"))
        )
        prov_rdd = prov_df.rdd.map(lambda row: _transform_provider(row, transformer))
        prov_df = spark.createDataFrame(prov_rdd)
        return prov_df

    return inner


def render_practitioner(spark: SparkSession, transformer: FHIRTransformer, seq: int = 1):
    transformer.load_template(PRAC_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        prac_df = (
            df.withColumn(
                "practitioner_internal_id",
                f.concat_ws("_", f.lit("Practitioner"), f.col("row_id"), f.lit(seq)),
            )
            .withColumn("practitioner_npi", get_column(df, "npi"))
            .select(df.colRegex("`practitioner_.*`"))
        )
        prac_rdd = prac_df.rdd.map(lambda row: _transform_practitioner(row, transformer))
        prac_df = spark.createDataFrame(prac_rdd)
        return prac_df

    return inner


def render_patient(
    spark: SparkSession, transformer: FHIRTransformer, seq: int = 1, prov_seq: int = 1, prac_seq: int = 1
):
    transformer.load_template(PAT_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        pat_df = (
            df.withColumn(
                "patient_internal_id",
                f.concat_ws("_", f.lit("Patient"), f.col("row_id"), f.lit(seq)),
            )
            .withColumn(
                "patient_organization_id",
                f.concat_ws("_", f.lit("Organization"), f.col("row_id"), f.lit(prov_seq)),
            )
            .withColumn(
                "patient_practitioner_id",
                f.concat_ws("_", f.lit("Practitioner"), f.col("row_id"), f.lit(prac_seq)),
            )
            .withColumn("patient_mbi", get_column(df, "bene_mbi_id"))
            .withColumn("patient_medicare", get_column(df, "bene_hic_num"))
            .withColumn("patient_first_name", get_column(df, "bene_1st_name"))
            .withColumn("patient_last_name", get_column(df, "bene_last_name"))
            .withColumn("patient_gender", get_column(df, "bene_sex_cd"))
            .withColumn("patient_dob", get_column(df, "bene_brth_dt"))
            .withColumn("patient_deceased_date", get_column(df, "bene_death_dt"))
            .withColumn("patient_state", get_column(df, "geo_ssa_state_name"))
            .withColumn("patient_district", get_column(df, "state_county_cd"))
            .withColumn("patient_race_code", get_column(df, "bene_race_cd"))
            .select(df.colRegex("`patient_.*`"))
        )
        pat_rdd = pat_df.rdd.map(lambda row: _transform_patient(row, transformer))
        pat_df = spark.createDataFrame(pat_rdd)
        return pat_df

    return inner


def render_coverage(spark: SparkSession, transformer: FHIRTransformer, seq: int = 1, pat_seq: int = 1):
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
            .withColumn("coverage_medicare_number", get_column(df, "bene_mbi_id"))
            .withColumn("coverage_cba_flag", get_column(df, "cba_flag"))
            .withColumn("coverage_assignment_type", get_column(df, "assignment_type"))
            .withColumn("coverage_assignment_before", get_column(df, "assignment_before"))
            .withColumn("coverage_asg_status", get_column(df, "asg_status"))
            .withColumn("coverage_partd_months", get_column(df, "partd_months"))
            .withColumn("coverage_excluded", get_column(df, "excluded"))
            .withColumn("coverage_deceased_excluded", get_column(df, "deceased_excluded"))
            .withColumn("coverage_missing_id_excluded", get_column(df, "missing_id_excluded"))
            .withColumn("coverage_part_a_b_only_excluded", get_column(df, "part_a_b_only_excluded"))
            .withColumn("coverage_ghp_excluded", get_column(df, "ghp_excluded"))
            .withColumn("coverage_outside_us_excluded", get_column(df, "outside_us_excluded"))
            .withColumn("coverage_other_shared_sav_init", get_column(df, "other_shared_sav_init"))
            .withColumn("coverage_enrollflag_1", get_column(df, "enrollflag_1"))
            .withColumn("coverage_enrollflag_2", get_column(df, "enrollflag_2"))
            .withColumn("coverage_enrollflag_3", get_column(df, "enrollflag_3"))
            .withColumn("coverage_enrollflag_4", get_column(df, "enrollflag_4"))
            .withColumn("coverage_enrollflag_5", get_column(df, "enrollflag_5"))
            .withColumn("coverage_enrollflag_6", get_column(df, "enrollflag_6"))
            .withColumn("coverage_enrollflag_7", get_column(df, "enrollflag_7"))
            .withColumn("coverage_enrollflag_8", get_column(df, "enrollflag_8"))
            .withColumn("coverage_enrollflag_9", get_column(df, "enrollflag_9"))
            .withColumn("coverage_enrollflag_10", get_column(df, "enrollflag_10"))
            .withColumn("coverage_enrollflag_11", get_column(df, "enrollflag_11"))
            .withColumn("coverage_enrollflag_12", get_column(df, "enrollflag_12"))
            .withColumn("coverage_bene_rsk_r_scre_01", get_column(df, "bene_rsk_r_scre_01"))
            .withColumn("coverage_bene_rsk_r_scre_02", get_column(df, "bene_rsk_r_scre_02"))
            .withColumn("coverage_bene_rsk_r_scre_03", get_column(df, "bene_rsk_r_scre_03"))
            .withColumn("coverage_bene_rsk_r_scre_04", get_column(df, "bene_rsk_r_scre_04"))
            .withColumn("coverage_bene_rsk_r_scre_05", get_column(df, "bene_rsk_r_scre_05"))
            .withColumn("coverage_bene_rsk_r_scre_06", get_column(df, "bene_rsk_r_scre_06"))
            .withColumn("coverage_bene_rsk_r_scre_07", get_column(df, "bene_rsk_r_scre_07"))
            .withColumn("coverage_bene_rsk_r_scre_08", get_column(df, "bene_rsk_r_scre_08"))
            .withColumn("coverage_bene_rsk_r_scre_09", get_column(df, "bene_rsk_r_scre_09"))
            .withColumn("coverage_bene_rsk_r_scre_10", get_column(df, "bene_rsk_r_scre_10"))
            .withColumn("coverage_bene_rsk_r_scre_11", get_column(df, "bene_rsk_r_scre_11"))
            .withColumn("coverage_bene_rsk_r_scre_12", get_column(df, "bene_rsk_r_scre_12"))
            .withColumn("coverage_esrd_score", get_column(df, "esrd_score"))
            .withColumn("coverage_dis_score", get_column(df, "dis_score"))
            .withColumn("coverage_agdu_score", get_column(df, "agdu_score"))
            .withColumn("coverage_agnd_score", get_column(df, "agnd_score"))
            .withColumn("coverage_dem_esrd_score", get_column(df, "dem_esrd_score"))
            .withColumn("coverage_dem_dis_score", get_column(df, "dem_dis_score"))
            .withColumn("coverage_dem_agdu_score", get_column(df, "dem_agdu_score"))
            .withColumn("coverage_dem_agnd_score", get_column(df, "dem_agnd_score"))
            .withColumn("coverage_new_enrollee", get_column(df, "new_enrollee"))
            .withColumn("coverage_lti_status", get_column(df, "lti_status"))
            .withColumn("coverage_va_selection_only", get_column(df, "va_selection_only"))
            .select(df.colRegex("`coverage_.*`"))
        )
        cov_rdd = cov_df.rdd.map(lambda row: _transform_coverage(row, transformer))
        cov_df = spark.createDataFrame(cov_rdd)
        return cov_df

    return inner


def render_condition(spark: SparkSession, transformer: FHIRTransformer, prac_seq: int = 1, pat_seq: int = 1):
    transformer.load_template(CON_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        con_df = (
            df.withColumn(
                "condition_internal_id",
                f.concat_ws("_", f.lit("Condition"), f.expr("uuid()")),
            )
            .withColumn(
                "condition_practitioner_id",
                f.concat_ws("_", f.lit("Practitioner"), f.col("row_id"), f.lit(prac_seq)),
            )
            .withColumn(
                "condition_patient_id",
                f.concat_ws("_", f.lit("Patient"), f.col("row_id"), f.lit(pat_seq)),
            )
            .withColumn("condition_hcc_code", get_column(df, "hcc_code"))
            .withColumn("condition_hcc_code_display", get_column(df, "hcc_code_display"))
            .select(df.colRegex("`condition_.*`"))
        )
        con_rdd = con_df.rdd.map(lambda row: _transform_condition(row, transformer))
        con_df = spark.createDataFrame(con_rdd)
        return con_df

    return inner


def to_fhir(spark: SparkSession, fhirbundle_landing_path: str, metadata: str = ""):
    def inner(df: DataFrame):
        # initialize fhir transformer
        transformer = FHIRTransformer()

        # prepares fhir:Provider
        resources_df = df.transform(render_provider(spark, transformer))

        # prepares fhir:Practitioner
        practitioner_df = df.transform(render_practitioner(spark, transformer))
        resources_df = resources_df.union(practitioner_df)

        # prepares fhir:Patient
        Patient_df = df.transform(render_patient(spark, transformer))
        resources_df = resources_df.union(Patient_df)

        # prepares fhir:Coverage
        coverage_df = df.transform(render_coverage(spark, transformer))
        resources_df = resources_df.union(coverage_df)

        # prepares fhir:Condition
        # filter hcc columns
        hcc_columns = [col for col in df.columns if "hcc_col_" in col]
        for col in hcc_columns:
            col_repr = col.split("_")[-1]
            df = df.withColumn(
                col, f.when(f.col(col) == 1, f.concat_ws("-", f.col("hcc_version"), f.lit(col_repr))).otherwise("")
            )

        hcc_df = (
            df.withColumn("hcc_ind_list", f.array(*hcc_columns))
            .withColumn("hcc_ind_list", f.array_distinct(f.expr("filter(hcc_ind_list, x -> x != '')")))
            .withColumn("hcc_ind", f.explode(f.col("hcc_ind_list")))
            .select("row_id", "bene_mbi_id", "hcc_ind")
        )
        # read master cms hcc codes
        master_value_set_file_path = "/app/data/cclf_hcc_master_value_set.csv"
        master_value_df = spark.read.csv(master_value_set_file_path, header=True)

        # Join final_df with master_value_df using a left join on the `hcc_category` column
        hcc_df = hcc_df.join(
            broadcast(master_value_df), hcc_df["hcc_ind"] == master_value_df["hcc_category"], "left"
        ).drop("hcc_ind", "hcc_category")
        condition_df = hcc_df.transform(render_condition(spark, transformer))
        resources_df = resources_df.union(condition_df)

        # collect all resources into a list and create fhir bundle for each file
        prepare_bundle_udf = f.udf(prepare_fhir_bundle)
        resources_df = resources_df.withColumn("resources", f.explode("resources")).agg(
            f.collect_list("resources").alias("resources")
        )
        resources_df = resources_df.withColumn("bundle_id", f.expr("uuid()"))
        fhir_df = resources_df.withColumn(
            "bundle",
            prepare_bundle_udf(f.col("bundle_id"), f.col("resources"), f.lit(fhirbundle_landing_path), f.lit(metadata)),
        ).drop("resources")
        return fhir_df

    return inner


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="cclf_beneficiary_stage_to_fhirbundle_job",
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
            exit_with_error(log, "delta schema location should be provided!")

        # construct delta table location
        alr1_1_delta_table_location = os.path.join(delta_schema_location, "cclf_bene_alr1_1")
        log.warn(f"alr1_1_delta_table_location: {alr1_1_delta_table_location}")

        alr1_2_delta_table_location = os.path.join(delta_schema_location, "cclf_bene_alr1_2")
        log.warn(f"alr1_2_delta_table_location: {alr1_2_delta_table_location}")

        alr1_3_delta_table_location = os.path.join(delta_schema_location, "cclf_bene_alr1_3")
        log.warn(f"alr1_3_delta_table_location: {alr1_3_delta_table_location}")

        alr1_4_delta_table_location = os.path.join(delta_schema_location, "cclf_bene_alr1_4")
        log.warn(f"alr1_4_delta_table_location: {alr1_4_delta_table_location}")

        alr1_5_delta_table_location = os.path.join(delta_schema_location, "cclf_bene_alr1_5")
        log.warn(f"alr1_5_delta_table_location: {alr1_5_delta_table_location}")

        alr1_6_delta_table_location = os.path.join(delta_schema_location, "cclf_bene_alr1_6")
        log.warn(f"alr1_6_delta_table_location: {alr1_6_delta_table_location}")

        alr1_7_delta_table_location = os.path.join(delta_schema_location, "cclf_bene_alr1_7")
        log.warn(f"alr1_7_delta_table_location: {alr1_7_delta_table_location}")

        alr1_8_delta_table_location = os.path.join(delta_schema_location, "cclf_bene_alr1_8")
        log.warn(f"alr1_8_delta_table_location: {alr1_8_delta_table_location}")

        alr1_9_delta_table_location = os.path.join(delta_schema_location, "cclf_bene_alr1_9")
        log.warn(f"alr1_9_delta_table_location: {alr1_9_delta_table_location}")

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
        spark = add_storage_context(
            spark,
            [
                alr1_1_delta_table_location,
                alr1_2_delta_table_location,
                alr1_3_delta_table_location,
                alr1_4_delta_table_location,
                alr1_5_delta_table_location,
                alr1_6_delta_table_location,
                alr1_7_delta_table_location,
                alr1_8_delta_table_location,
                alr1_9_delta_table_location,
            ],
        )

        # load the records from delta table location
        log.warn("load records from alr1_1 delta table location")
        alr1_1_df = (
            spark.read.format("delta")
            .load(alr1_1_delta_table_location)
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

        if alr1_1_df.isEmpty():
            log.warn(f"Spark job {app_name} completed successfully, There is lack of adequate rows for processing.")
            sys.exit(0)

        log.warn("load records from alr1_2 delta table location")
        alr1_2_df = (
            spark.read.format("delta")
            .load(alr1_2_delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .withColumn("pcs_count", f.col("b_em_line_cnt_t"))
            .select("bene_mbi_id", "master_id", "pcs_count")
            .fillna("")
        )

        log.warn("load records from alr1_3 delta table location")
        alr1_3_df = (
            spark.read.format("delta")
            .load(alr1_3_delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .withColumn("pcs_count", f.col("rev_line_cnt"))
            .select("bene_mbi_id", "master_id", "pcs_count")
            .fillna("")
        )

        log.warn("load records from alr1_4 delta table location")
        alr1_4_df = (
            spark.read.format("delta")
            .load(alr1_4_delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .withColumn("npi", f.col("npi_used"))
            .select("bene_mbi_id", "master_id", "npi", "pcs_count")
            .fillna("")
        )

        pcs_df = (
            alr1_4_df.select("bene_mbi_id", "master_id", "pcs_count")
            .union(alr1_2_df)
            .union(alr1_3_df)
            .drop_duplicates(["bene_mbi_id"])
        )

        # log.warn("load records from alr1_5 delta table location")
        # alr1_5_df = (
        #     spark.read.format("delta").load(alr1_5_delta_table_location).filter(
        #         f.col("file_batch_id") == file_batch_id)
        # )

        log.warn("load records from alr1_6 delta table location")
        alr1_6_df = (
            spark.read.format("delta")
            .load(alr1_6_delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .select("bene_mbi_id", "va_selection_only")
            .fillna("")
        )

        # log.warn("load records from alr1_7 delta table location")
        # alr1_7_df = (
        #     spark.read.format("delta")
        #     .load(alr1_7_delta_table_location)
        #     .filter(f.col("file_batch_id") == file_batch_id)
        #     .withColumn("covid19_b9729", f.col("b9729"))
        #     .withColumn("covid19_u071", f.col("u071"))
        #     .drop("b9729", "u071", "row_id")
        # )

        # log.warn("load records from alr1_8 delta table location")
        # alr1_8_df = (
        #     spark.read.format("delta").load(alr1_8_delta_table_location).filter(
        #           f.col("file_batch_id") == file_batch_id)
        # )

        # log.warn("load records from alr1_9 delta table location")
        # alr1_9_df = (
        #     spark.read.format("delta")
        #     .load(alr1_9_delta_table_location)
        #     .filter(f.col("file_batch_id") == file_batch_id)
        #     .drop("row_id")
        # )

        # join tables from alr1_1 to alr1_9
        data_df = (
            alr1_1_df.join(pcs_df, on="bene_mbi_id", how="left")
            .join(alr1_4_df.select("bene_mbi_id", "npi").drop_duplicates(["bene_mbi_id"]), on="bene_mbi_id", how="left")
            .join(alr1_6_df, on="bene_mbi_id", how="left")
            # .join(alr1_7_df, on="bene_mbi_id", how="left")
            # .join(alr1_9_df, on="bene_mbi_id", how="left")
            .fillna("")
        )

        # transform into fhir bundle and write into target location
        fhir_df = data_df.transform(to_fhir(spark, fhirbundle_landing_path, json.dumps(metadata)))
        fhir_df.head(1)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"from file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
