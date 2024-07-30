import os

import click

from dependencies.spark import add_storage_context, start_spark
from utils.utils import exit_with_error, load_config


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="delta_load_on_request_job",
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
        # remove default analytical schema
        # ex. `s3a://phm-development-datapipeline-bucket/delta-tables/nucleo/analytical` changed to
        # `s3a://phm-development-datapipeline-bucket/delta-tables/nucleo`
        delta_schema_location = "/".join(delta_schema_location.split("/")[:-1])

        data_file_path = os.environ.get("DATA_FILE_PATH", config.get("data_file_path", ""))
        if not data_file_path:
            exit_with_error(log, "data file path should be provided!")
        log.warn(f"data_file_path: {data_file_path}")

        file_tenant = os.environ.get("FILE_TENANT", config.get("file_tenant", "nucleo"))
        if not file_tenant:
            exit_with_error(log, "file tenant should be provided!")
        log.warn(f"file_tenant: {file_tenant}")

        delta_schema = os.environ.get("DELTA_SCHEMA", config.get("delta_schema", ""))
        if not delta_schema:
            exit_with_error(log, "delta schema should be provided!")
        log.warn(f"delta_schema: {delta_schema}")

        delta_table = os.environ.get("DELTA_TABLE", config.get("delta_table", ""))
        if not delta_table:
            exit_with_error(log, "delta table should be provided!")
        log.warn(f"delta_table: {delta_table}")

        delimiter = os.environ.get("DELIMITER", config.get("delimiter", ","))
        log.warn(f"delimiter: {delimiter}")

        mode = os.environ.get("MODE", config.get("mode", "append"))
        log.warn(f"mode: {mode}")

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_schema, delta_table)
        log.warn(f"delta_table_location: {delta_table_location}")

        # add storage context in spark session
        spark = add_storage_context(spark, [data_file_path, delta_table_location])

        # load the record keys file
        log.warn("load canonical file into dataframe")
        data_df = spark.read.csv(data_file_path, header=True, sep=delimiter)

        # write data into delta lake practice table
        data_df.write.format("delta").mode(mode).save(delta_table_location)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{data_file_path} loaded into delta table path {delta_table_location}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
