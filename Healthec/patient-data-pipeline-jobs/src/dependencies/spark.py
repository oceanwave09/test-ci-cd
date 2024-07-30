from typing import List
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DataType, StringType, StructType
from pyspark.sql.utils import AnalysisException

from dependencies import logging
from utils.constants import S3_SCHEMA


def start_spark(
    app_name: str = "hec_data_pipeline_job",
    spark_config={},
    log_level="WARN",
):
    """Start spark session and also configure logger format with app details.

    :param  app_name: Name of Spark app
    :return: A tuple of spark session and logger references
    """
    builder = SparkSession.builder.appName(app_name)
    # add spark configs
    for key, val in spark_config.items():
        builder.config(key, val)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    logger = logging.Log4j(spark)

    return spark, logger


def add_storage_context(spark: SparkSession, file_paths: List) -> SparkSession:
    for file_path in file_paths:
        if S3_SCHEMA == urlparse(file_path).scheme:
            spark = _set_s3_credentials_provider(spark)
            break
    return spark


def _set_s3_credentials_provider(
    spark: SparkSession,
) -> SparkSession:
    # set AWS web identity token credentials provider in spark context
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    )
    # set AWS S3 configuration in spark context
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.connection.ssl.enabled",
        "true",
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.endpoint",
        "s3.amazonaws.com",
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.fast.upload",
        "true",
    )
    return spark


def flatten(df):
    complex_fields = dict(
        [
            (field.name, field.dataType)
            for field in df.schema.fields
            if type(field.dataType) == StructType
        ]
    )
    if len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]

        if type(complex_fields[col_name]) == StructType:
            expanded = [
                f.col(col_name + "." + k).alias(k)
                for k in [n.name for n in complex_fields[col_name]]
            ]
            df = df.select("*", *expanded).drop(col_name)
    return df


def get_column(
    df: DataFrame,
    column: str,
    dtype: DataType = StringType(),
):
    try:
        return df[column].cast(dtype)
    except AnalysisException:
        return f.lit(None).cast(dtype)


def is_column_exists(df: DataFrame, column: str) -> bool:
    try:
        df[column]
        return True
    except AnalysisException:
        return False
