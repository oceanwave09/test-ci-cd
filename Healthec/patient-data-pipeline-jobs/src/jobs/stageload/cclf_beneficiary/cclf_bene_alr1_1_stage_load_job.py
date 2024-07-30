import os

import click
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, StringType

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.constants import INGESTION_PIPELINE_USER
from utils.enums import RecordStatus as rs
from utils.transformation import parse_date, to_int
from utils.utils import exit_with_error, generate_random_string, load_config


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="cclf_bene_alr1_1_stage_load_job",
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
            config.get("delta_table_name", "cclf_bene_alr1_1"),
        )
        if not delta_table_name:
            exit_with_error(
                log,
                "delta table location should be provided!",
            )

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_table_name)
        log.warn(f"delta_table_location: {delta_table_location}")

        canonical_file_path = os.environ.get(
            "CANONICAL_FILE_PATH",
            config.get("canonical_file_path", ""),
        )
        if not canonical_file_path:
            exit_with_error(
                log,
                "canonical file path should be provided!",
            )
        log.warn(f"canonical_file_path: {canonical_file_path}")

        file_batch_id = os.environ.get(
            "FILE_BATCH_ID",
            config.get(
                "file_batch_id",
                f"BATCH_{generate_random_string()}",
            ),
        )
        log.warn(f"file_batch_id: {file_batch_id}")

        file_source = os.environ.get("FILE_SOURCE", config.get("file_source", None))
        log.warn(f"file_source: {file_source}")

        file_name = os.environ.get("FILE_NAME", config.get("file_name", None))
        log.warn(f"file_name: {file_name}")

        # add storage context in spark session
        spark = add_storage_context(
            spark,
            [canonical_file_path, delta_table_location],
        )

        # Register function as UDF with Spark
        parse_date_udf = f.udf(parse_date, StringType())
        to_int_udf = f.udf(to_int, IntegerType())

        # load the record keys file
        log.warn("load canonical file into dataframe")
        data_df = spark.read.json(canonical_file_path, multiLine=True)

        # add additional columns and prepare dataframe which match with cclf_bene_asgd_benes delta lake table structure
        alr1_1_df = (
            data_df.withColumn("row_id", get_column(data_df, "row_id"))
            .withColumn("bene_mbi_id", get_column(data_df, "bene_mbi_id"))
            .withColumn("bene_hic_num", get_column(data_df, "bene_hic_num"))
            .withColumn("bene_1st_name", get_column(data_df, "bene_1st_name"))
            .withColumn("bene_last_name", get_column(data_df, "bene_last_name"))
            .withColumn("bene_sex_cd", get_column(data_df, "bene_sex_cd"))
            .withColumn("bene_brth_dt", f.to_date(parse_date_udf(get_column(data_df, "bene_brth_dt"))))
            .withColumn("bene_death_dt", f.to_date(parse_date_udf(get_column(data_df, "bene_death_dt"))))
            .withColumn("geo_ssa_cnty_cd_name", get_column(data_df, "geo_ssa_cnty_cd_name"))
            .withColumn("geo_ssa_state_name", get_column(data_df, "geo_ssa_state_name"))
            .withColumn("state_county_cd", get_column(data_df, "state_county_cd"))
            .withColumn("in_va_max", to_int_udf(get_column(data_df, "in_va_max")))
            .withColumn("va_tin", get_column(data_df, "va_tin"))
            .withColumn("va_npi", get_column(data_df, "va_npi"))
            .withColumn("cba_flag", to_int_udf(get_column(data_df, "cba_flag")))
            .withColumn("assignment_type", to_int_udf(get_column(data_df, "assignment_type")))
            .withColumn("assigned_before", to_int_udf(get_column(data_df, "assigned_before")))
            .withColumn("asg_status", to_int_udf(get_column(data_df, "asg_status")))
            .withColumn("partd_months", to_int_udf(get_column(data_df, "partd_months")))
            .withColumn("excluded", to_int_udf(get_column(data_df, "excluded")))
            .withColumn("deceased_excluded", to_int_udf(get_column(data_df, "deceased_excluded")))
            .withColumn("missing_id_excluded", to_int_udf(get_column(data_df, "missing_id_excluded")))
            .withColumn("part_a_b_only_excluded", to_int_udf(get_column(data_df, "part_a_b_only_excluded")))
            .withColumn("ghp_excluded", to_int_udf(get_column(data_df, "ghp_excluded")))
            .withColumn("outside_us_excluded", to_int_udf(get_column(data_df, "outside_us_excluded")))
            .withColumn("other_shared_sav_init", to_int_udf(get_column(data_df, "other_shared_sav_init")))
            .withColumn("enrollflag_1", to_int_udf(get_column(data_df, "enrollflag_1")))
            .withColumn("enrollflag_2", to_int_udf(get_column(data_df, "enrollflag_2")))
            .withColumn("enrollflag_3", to_int_udf(get_column(data_df, "enrollflag_3")))
            .withColumn("enrollflag_4", to_int_udf(get_column(data_df, "enrollflag_4")))
            .withColumn("enrollflag_5", to_int_udf(get_column(data_df, "enrollflag_5")))
            .withColumn("enrollflag_6", to_int_udf(get_column(data_df, "enrollflag_6")))
            .withColumn("enrollflag_7", to_int_udf(get_column(data_df, "enrollflag_7")))
            .withColumn("enrollflag_8", to_int_udf(get_column(data_df, "enrollflag_8")))
            .withColumn("enrollflag_9", to_int_udf(get_column(data_df, "enrollflag_9")))
            .withColumn("enrollflag_10", to_int_udf(get_column(data_df, "enrollflag_10")))
            .withColumn("enrollflag_11", to_int_udf(get_column(data_df, "enrollflag_11")))
            .withColumn("enrollflag_12", to_int_udf(get_column(data_df, "enrollflag_12")))
        )

        alr1_1_df = (
            alr1_1_df.withColumn("hcc_version", get_column(alr1_1_df, "hcc_version"))
            .withColumn("hcc_col_1", get_column(alr1_1_df, "hcc_col_1"))
            .withColumn("hcc_col_2", get_column(alr1_1_df, "hcc_col_2"))
            .withColumn("hcc_col_3", get_column(alr1_1_df, "hcc_col_3"))
            .withColumn("hcc_col_4", get_column(alr1_1_df, "hcc_col_4"))
            .withColumn("hcc_col_5", get_column(alr1_1_df, "hcc_col_5"))
            .withColumn("hcc_col_6", get_column(alr1_1_df, "hcc_col_6"))
            .withColumn("hcc_col_7", get_column(alr1_1_df, "hcc_col_7"))
            .withColumn("hcc_col_8", get_column(alr1_1_df, "hcc_col_8"))
            .withColumn("hcc_col_9", get_column(alr1_1_df, "hcc_col_9"))
            .withColumn("hcc_col_10", get_column(alr1_1_df, "hcc_col_10"))
            .withColumn("hcc_col_11", get_column(alr1_1_df, "hcc_col_11"))
            .withColumn("hcc_col_12", get_column(alr1_1_df, "hcc_col_12"))
            .withColumn("hcc_col_13", get_column(alr1_1_df, "hcc_col_13"))
            .withColumn("hcc_col_14", get_column(alr1_1_df, "hcc_col_14"))
            .withColumn("hcc_col_15", get_column(alr1_1_df, "hcc_col_15"))
            .withColumn("hcc_col_16", get_column(alr1_1_df, "hcc_col_16"))
            .withColumn("hcc_col_17", get_column(alr1_1_df, "hcc_col_17"))
            .withColumn("hcc_col_18", get_column(alr1_1_df, "hcc_col_18"))
            .withColumn("hcc_col_19", get_column(alr1_1_df, "hcc_col_19"))
            .withColumn("hcc_col_20", get_column(alr1_1_df, "hcc_col_20"))
            .withColumn("hcc_col_21", get_column(alr1_1_df, "hcc_col_21"))
            .withColumn("hcc_col_22", get_column(alr1_1_df, "hcc_col_22"))
            .withColumn("hcc_col_23", get_column(alr1_1_df, "hcc_col_23"))
            .withColumn("hcc_col_24", get_column(alr1_1_df, "hcc_col_24"))
            .withColumn("hcc_col_25", get_column(alr1_1_df, "hcc_col_25"))
            .withColumn("hcc_col_26", get_column(alr1_1_df, "hcc_col_26"))
            .withColumn("hcc_col_27", get_column(alr1_1_df, "hcc_col_27"))
            .withColumn("hcc_col_28", get_column(alr1_1_df, "hcc_col_28"))
            .withColumn("hcc_col_29", get_column(alr1_1_df, "hcc_col_29"))
            .withColumn("hcc_col_30", get_column(alr1_1_df, "hcc_col_30"))
            .withColumn("hcc_col_31", get_column(alr1_1_df, "hcc_col_31"))
            .withColumn("hcc_col_32", get_column(alr1_1_df, "hcc_col_32"))
            .withColumn("hcc_col_33", get_column(alr1_1_df, "hcc_col_33"))
            .withColumn("hcc_col_34", get_column(alr1_1_df, "hcc_col_34"))
            .withColumn("hcc_col_35", get_column(alr1_1_df, "hcc_col_35"))
            .withColumn("hcc_col_36", get_column(alr1_1_df, "hcc_col_36"))
            .withColumn("hcc_col_37", get_column(alr1_1_df, "hcc_col_37"))
            .withColumn("hcc_col_38", get_column(alr1_1_df, "hcc_col_38"))
            .withColumn("hcc_col_39", get_column(alr1_1_df, "hcc_col_39"))
            .withColumn("hcc_col_40", get_column(alr1_1_df, "hcc_col_40"))
            .withColumn("hcc_col_41", get_column(alr1_1_df, "hcc_col_41"))
            .withColumn("hcc_col_42", get_column(alr1_1_df, "hcc_col_42"))
            .withColumn("hcc_col_43", get_column(alr1_1_df, "hcc_col_43"))
            .withColumn("hcc_col_44", get_column(alr1_1_df, "hcc_col_44"))
            .withColumn("hcc_col_45", get_column(alr1_1_df, "hcc_col_45"))
            .withColumn("hcc_col_46", get_column(alr1_1_df, "hcc_col_46"))
            .withColumn("hcc_col_47", get_column(alr1_1_df, "hcc_col_47"))
            .withColumn("hcc_col_48", get_column(alr1_1_df, "hcc_col_48"))
            .withColumn("hcc_col_49", get_column(alr1_1_df, "hcc_col_49"))
            .withColumn("hcc_col_50", get_column(alr1_1_df, "hcc_col_50"))
            .withColumn("hcc_col_51", get_column(alr1_1_df, "hcc_col_51"))
            .withColumn("hcc_col_52", get_column(alr1_1_df, "hcc_col_52"))
            .withColumn("hcc_col_53", get_column(alr1_1_df, "hcc_col_53"))
            .withColumn("hcc_col_54", get_column(alr1_1_df, "hcc_col_54"))
            .withColumn("hcc_col_55", get_column(alr1_1_df, "hcc_col_55"))
            .withColumn("hcc_col_56", get_column(alr1_1_df, "hcc_col_56"))
            .withColumn("hcc_col_57", get_column(alr1_1_df, "hcc_col_57"))
            .withColumn("hcc_col_58", get_column(alr1_1_df, "hcc_col_58"))
            .withColumn("hcc_col_59", get_column(alr1_1_df, "hcc_col_59"))
            .withColumn("hcc_col_60", get_column(alr1_1_df, "hcc_col_60"))
            .withColumn("hcc_col_61", get_column(alr1_1_df, "hcc_col_61"))
            .withColumn("hcc_col_62", get_column(alr1_1_df, "hcc_col_62"))
            .withColumn("hcc_col_63", get_column(alr1_1_df, "hcc_col_63"))
            .withColumn("hcc_col_64", get_column(alr1_1_df, "hcc_col_64"))
            .withColumn("hcc_col_65", get_column(alr1_1_df, "hcc_col_65"))
            .withColumn("hcc_col_66", get_column(alr1_1_df, "hcc_col_66"))
            .withColumn("hcc_col_67", get_column(alr1_1_df, "hcc_col_67"))
            .withColumn("hcc_col_68", get_column(alr1_1_df, "hcc_col_68"))
            .withColumn("hcc_col_69", get_column(alr1_1_df, "hcc_col_69"))
            .withColumn("hcc_col_70", get_column(alr1_1_df, "hcc_col_70"))
            .withColumn("hcc_col_71", get_column(alr1_1_df, "hcc_col_71"))
            .withColumn("hcc_col_72", get_column(alr1_1_df, "hcc_col_72"))
            .withColumn("hcc_col_73", get_column(alr1_1_df, "hcc_col_73"))
            .withColumn("hcc_col_74", get_column(alr1_1_df, "hcc_col_74"))
            .withColumn("hcc_col_75", get_column(alr1_1_df, "hcc_col_75"))
            .withColumn("hcc_col_76", get_column(alr1_1_df, "hcc_col_76"))
            .withColumn("hcc_col_77", get_column(alr1_1_df, "hcc_col_77"))
            .withColumn("hcc_col_78", get_column(alr1_1_df, "hcc_col_78"))
            .withColumn("hcc_col_79", get_column(alr1_1_df, "hcc_col_79"))
            .withColumn("hcc_col_80", get_column(alr1_1_df, "hcc_col_80"))
            .withColumn("hcc_col_81", get_column(alr1_1_df, "hcc_col_81"))
            .withColumn("hcc_col_82", get_column(alr1_1_df, "hcc_col_82"))
            .withColumn("hcc_col_83", get_column(alr1_1_df, "hcc_col_83"))
            .withColumn("hcc_col_84", get_column(alr1_1_df, "hcc_col_84"))
            .withColumn("hcc_col_85", get_column(alr1_1_df, "hcc_col_85"))
            .withColumn("hcc_col_86", get_column(alr1_1_df, "hcc_col_86"))
            .withColumn("hcc_col_87", get_column(alr1_1_df, "hcc_col_87"))
            .withColumn("hcc_col_88", get_column(alr1_1_df, "hcc_col_88"))
            .withColumn("hcc_col_89", get_column(alr1_1_df, "hcc_col_89"))
            .withColumn("hcc_col_90", get_column(alr1_1_df, "hcc_col_90"))
            .withColumn("bene_rsk_r_scre_01", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_01")))
            .withColumn("bene_rsk_r_scre_02", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_02")))
            .withColumn("bene_rsk_r_scre_03", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_03")))
            .withColumn("bene_rsk_r_scre_04", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_04")))
            .withColumn("bene_rsk_r_scre_05", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_05")))
            .withColumn("bene_rsk_r_scre_06", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_06")))
            .withColumn("bene_rsk_r_scre_07", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_07")))
            .withColumn("bene_rsk_r_scre_08", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_08")))
            .withColumn("bene_rsk_r_scre_09", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_09")))
            .withColumn("bene_rsk_r_scre_10", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_10")))
            .withColumn("bene_rsk_r_scre_11", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_11")))
            .withColumn("bene_rsk_r_scre_12", to_int_udf(get_column(alr1_1_df, "bene_rsk_r_scre_12")))
            .withColumn("esrd_score", to_int_udf(get_column(alr1_1_df, "esrd_score")))
            .withColumn("dis_score", to_int_udf(get_column(alr1_1_df, "dis_score")))
            .withColumn("agdu_score", to_int_udf(get_column(alr1_1_df, "agdu_score")))
            .withColumn("agnd_score", to_int_udf(get_column(alr1_1_df, "agnd_score")))
            .withColumn("dem_esrd_score", to_int_udf(get_column(alr1_1_df, "dem_esrd_score")))
            .withColumn("dem_dis_score", to_int_udf(get_column(alr1_1_df, "dem_dis_score")))
            .withColumn("dem_agdu_score", to_int_udf(get_column(alr1_1_df, "dem_agdu_score")))
            .withColumn("dem_agnd_score", to_int_udf(get_column(alr1_1_df, "dem_agnd_score")))
            .withColumn("new_enrollee", to_int_udf(get_column(alr1_1_df, "new_enrollee")))
            .withColumn("lti_status", get_column(alr1_1_df, "lti_status"))
            .withColumn("bene_race_cd", get_column(alr1_1_df, "bene_race_cd"))
            .withColumn("file_batch_id", f.lit(file_batch_id).cast(StringType()))
            .withColumn("file_name", f.lit(file_name).cast(StringType()))
            .withColumn("file_source_name", f.lit(file_source).cast(StringType()))
            .withColumn("file_status", f.lit(rs.STAGE_LOAD.value))
            .withColumn("created_user", f.lit(INGESTION_PIPELINE_USER).cast(StringType()))
            .withColumn("created_ts", f.current_timestamp())
            .withColumn("updated_user", f.lit(INGESTION_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        # write data into delta lake `cclf_bene_asgd_benes` table
        alr1_1_df.write.format("delta").mode("append").save(delta_table_location)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{canonical_file_path} loaded into stage table path {delta_table_location}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
