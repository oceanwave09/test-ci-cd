import json
import os
from datetime import datetime
from urllib.parse import urlparse

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.measure import MEASURE_SCHEMA
from utils.api_client import get_measure
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def _transform_measure(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # get organization, measure and patient id from links in event message
    links = row_dict.get("links")
    links_json = json.loads(links.replace("'", '"')) if links else {}
    url = links_json.get("self", "")
    parsed_link = urlparse(url)
    path_split = parsed_link.path.split("/")
    organization_id = ""
    measure_id = ""
    patient_id = ""
    date = ""
    if len(path_split) == 10:
        organization_id = path_split[4]
        measure_id = path_split[6]
        date = path_split[7]
        patient_id = path_split[9].replace("}", "")

    # get patient measure details from platform service
    measure_response = get_measure(organization_id, measure_id, patient_id, date)

    # # get patient details from platform service
    # patient_resource = get_core_entity(patient_id, Patient)

    # construct measure fields
    measure_dict = {}
    measure_dict["patient_fhir_id"] = measure_response.get("patientId")
    measure_dict["practice_fhir_id"] = measure_response.get("organizationId")
    measure_dict["provider_fhir_id"] = None
    measure_dict["coverage_fhir_id"] = None
    measure_dict["payer_fhir_id"] = None
    measure_dict["plan_fhir_id"] = None
    measure_dict["measure_id"] = measure_response.get("measureId")
    measure_groups = measure_response.get("groups", [])
    measure_dict["group_key"] = measure_groups[0].get("key") if len(measure_groups) > 0 else None
    measure_dict["numerator"] = measure_groups[0].get("numerator") if len(measure_groups) > 0 else None
    measure_dict["numerator_exclusion"] = (
        measure_groups[0].get("numeratorExclusion") if len(measure_groups) > 0 else None
    )
    measure_dict["numerator_exception"] = (
        measure_groups[0].get("numeratorException") if len(measure_groups) > 0 else None
    )
    measure_dict["denominator"] = measure_groups[0].get("denominator") if len(measure_groups) > 0 else None
    measure_dict["denominator_exclusion"] = (
        measure_groups[0].get("denominatorExclusion") if len(measure_groups) > 0 else None
    )
    measure_dict["denominator_exception"] = (
        measure_groups[0].get("denominatorException") if len(measure_groups) > 0 else None
    )
    measure_dict["initial_population"] = (
        measure_groups[0].get("initialPopulation") if len(measure_groups) > 0 else None
    )
    from_date = None
    to_date = None
    raw_results = measure_response.get("rawResults", [])
    for result in raw_results:
        if result.get("key", "") == "Period":
            from_date = result.get("value", {}).get("low")
            to_date = result.get("value", {}).get("high")
            break
    measure_dict["from_date"] = datetime.strptime(from_date, "%Y-%m-%d") if from_date else None
    measure_dict["to_date"] = datetime.strptime(to_date, "%Y-%m-%d") if to_date else None

    return Row(**measure_dict)


def update_measure_table(spark: SparkSession, delta_schema_location: str):
    def inner(df: DataFrame):
        measure_rdd = df.rdd.map(lambda row: _transform_measure(row))
        measure_df = spark.createDataFrame(measure_rdd, schema=MEASURE_SCHEMA)
        measure_df = (
            measure_df.filter(f.col("patient_fhir_id").isNotNull())
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )
        measure_df.persist()

        if measure_df.count() > 0:
            # expose final dataframe into temp view
            temp_view = f"measure_updates_{generate_random_string()}"
            measure_df.createOrReplaceTempView(temp_view)
            delta_table_location = os.path.join(delta_schema_location, "measure")

            # build and run merge query
            merge_query = f"""MERGE INTO delta.`{delta_table_location}`
            USING {temp_view}
            ON delta.`{delta_table_location}`.patient_fhir_id = {temp_view}.patient_fhir_id
            AND delta.`{delta_table_location}`.practice_fhir_id = {temp_view}.practice_fhir_id
            AND delta.`{delta_table_location}`.measure_id = {temp_view}.measure_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *;
            """
            spark.sql(merge_query)
        return measure_df

    return inner
