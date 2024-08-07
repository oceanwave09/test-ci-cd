import os

import click
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, LongType, StringType, IntegerType

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.constants import INGESTION_PIPELINE_USER
from utils.enums import RecordStatus as rs
from utils.transformation import parse_date, to_float, to_int
from utils.utils import exit_with_error, generate_random_string, load_config


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="cclfa_stage_load_job",
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
            config.get("delta_table_name", "cclfa"),
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
        to_float_udf = f.udf(to_float, DoubleType())
        to_int_udf = f.udf(to_int, IntegerType())
        to_long_udf = f.udf(to_int, LongType())

        # load the record keys file
        log.warn("load canonical file into dataframe")
        data_df = spark.read.json(canonical_file_path, multiLine=True)

        # add additional columns and prepare dataframe which match with cclfa delta lake table structure
        cclfa_df = (
            data_df.withColumn("row_id", get_column(data_df, "row_id"))
            .withColumn("cur_clm_uniq_id", to_long_udf(get_column(data_df, "cur_clm_uniq_id")))
            .withColumn("bene_mbi_id", get_column(data_df, "bene_mbi_id"))
            .withColumn("bene_hic_num", get_column(data_df, "bene_hic_num"))
            .withColumn("clm_type_cd", to_int_udf(get_column(data_df, "clm_type_cd")))
            .withColumn("clm_actv_care_from_dt", f.to_date(parse_date_udf(get_column(data_df, "clm_actv_care_from_dt"))))
            .withColumn("clm_ngaco_pbpmt_sw", get_column(data_df, "clm_ngaco_pbpmt_sw"))
            .withColumn("clm_ngaco_pdschrg_hcbs_sw", get_column(data_df, "clm_ngaco_pdschrg_hcbs_sw"))
            .withColumn("clm_ngaco_snf_wvr_sw", get_column(data_df, "clm_ngaco_snf_wvr_sw"))
            .withColumn("clm_ngaco_tlhlth_sw", get_column(data_df, "clm_ngaco_tlhlth_sw"))
            .withColumn("clm_ngaco_cptatn_sw", get_column(data_df, "clm_ngaco_cptatn_sw"))
            .withColumn("clm_demo_1st_num", get_column(data_df, "clm_demo_1st_num"))
            .withColumn("clm_demo_2nd_num", get_column(data_df, "clm_demo_2nd_num"))
            .withColumn("clm_demo_3rd_num", get_column(data_df, "clm_demo_3rd_num"))
            .withColumn("clm_demo_4th_num", get_column(data_df, "clm_demo_4th_num"))
            .withColumn("clm_demo_5th_num", get_column(data_df, "clm_demo_5th_num"))
            .withColumn("clm_pbp_inclsn_amt", to_float_udf(get_column(data_df, "clm_pbp_inclsn_amt")))
            .withColumn("clm_pbp_rdctn_amt", to_float_udf(get_column(data_df, "clm_pbp_rdctn_amt")))
            .withColumn("clm_ngaco_cmg_wvr_sw", get_column(data_df, "clm_ngaco_cmg_wvr_sw"))
            .withColumn("clm_instnl_per_diem_amt", to_float_udf(get_column(data_df, "clm_instnl_per_diem_amt")))
            .withColumn("clm_mdcr_ip_bene_ddctbl_amt", to_float_udf(get_column(data_df, "clm_mdcr_ip_bene_ddctbl_amt")))
            .withColumn("clm_mdcr_coinsrnc_amt", to_float_udf(get_column(data_df, "clm_mdcr_coinsrnc_amt")))
            .withColumn("clm_blood_lblty_amt", to_float_udf(get_column(data_df, "clm_blood_lblty_amt")))
            .withColumn("clm_instnl_prfnl_amt", to_float_udf(get_column(data_df, "clm_instnl_prfnl_amt")))
            .withColumn("clm_ncvrd_chrg_amt", to_float_udf(get_column(data_df, "clm_ncvrd_chrg_amt")))
            .withColumn("clm_mdcr_ddctbl_amt", to_float_udf(get_column(data_df, "clm_mdcr_ddctbl_amt")))
            .withColumn("clm_rlt_cond_cd", get_column(data_df, "clm_rlt_cond_cd"))
            .withColumn("clm_oprtnl_outlr_amt", to_float_udf(get_column(data_df, "clm_oprtnl_outlr_amt")))
            .withColumn("clm_mdcr_new_tech_amt", to_float_udf(get_column(data_df, "clm_mdcr_new_tech_amt")))
            .withColumn("clm_islet_isoln_amt", to_float_udf(get_column(data_df, "clm_islet_isoln_amt")))
            .withColumn("clm_sqstrtn_rdctn_amt", to_float_udf(get_column(data_df, "clm_sqstrtn_rdctn_amt")))
            .withColumn("clm_1_rev_cntr_ansi_rsn_cd", get_column(data_df, "clm_1_rev_cntr_ansi_rsn_cd"))
            .withColumn("clm_1_rev_cntr_ansi_grp_cd", get_column(data_df, "clm_1_rev_cntr_ansi_grp_cd"))
            .withColumn("clm_mips_pmt_amt", to_float_udf(get_column(data_df, "clm_mips_pmt_amt")))
            .withColumn("file_batch_id", f.lit(file_batch_id).cast(StringType()))
            .withColumn("file_name", f.lit(file_name).cast(StringType()))
            .withColumn("file_source_name", f.lit(file_source).cast(StringType()))
            .withColumn("file_status", f.lit(rs.STAGE_LOAD.value))
            .withColumn("created_user", f.lit(INGESTION_PIPELINE_USER).cast(StringType()))
            .withColumn("created_ts", f.current_timestamp())
            .withColumn("updated_user", f.lit(INGESTION_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        # write data into delta lake `cclfa` table
        cclfa_df.write.format("delta").mode("append").save(delta_table_location)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{canonical_file_path} loaded into stage table path {delta_table_location}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
