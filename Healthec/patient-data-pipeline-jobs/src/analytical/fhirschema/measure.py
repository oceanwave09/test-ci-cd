from pyspark.sql.types import ArrayType, BooleanType, DateType, IntegerType, StringType, StructField, StructType

# import json

"""
Schema for measure analytical table : schema will be based on the fhir template.
"""
# fhir json structure
# measure_json = {
#   "patient_fhir_id": "ee04ec2b-5fe5-4ada-9e2f-6f09afb200da",
#   "practice_fhir_id": "4b57964b-c487-4904-84fd-7ccd4333b686",
#   "provider_fhir_id": ["0ac32572-161f-4527-a481-b66a8fccbbf3"],
#   "coverage_fhir_id": "4b57964b-c487-4904-84fd-7ccd4333b686",
#   "payer_fhir_id": "4b57964b-c487-4904-84fd-7ccd4333b686",
#   "plan_fhir_id": "4b57964b-c487-4904-84fd-7ccd4333b686",
#   "measure_id": "4b57964b-c487-4904-84fd-7ccd4333b686",
#   "group_key": 1,
#   "numerator": true,
#   "numerator_exclusion": false,
#   "numerator_exception": false,
#   "denominator": true,
#   "denominator_exclusion": false,
#   "denominator_exception": false,
#   "initial_population": false,
#   "from_date": "2022-01-01",
#   "to_date": "2022-12-31",
# }

# build schema
# df=spark.read.json(spark.sparkContext.parallelize([measure_json]))
# schema_json = df.schema.json()
# MEASURE_SCHEMA = StructType.fromJson(json.loads(measure_json))

MEASURE_SCHEMA = StructType(
    [
        StructField("patient_fhir_id", StringType(), True),
        StructField("practice_fhir_id", StringType(), True),
        StructField("provider_fhir_id", ArrayType(StringType()), True),
        StructField("coverage_fhir_id", StringType(), True),
        StructField("payer_fhir_id", StringType(), True),
        StructField("plan_fhir_id", StringType(), True),
        StructField("measure_id", StringType(), True),
        StructField("group_key", IntegerType(), True),
        StructField("numerator", BooleanType(), True),
        StructField("numerator_exclusion", BooleanType(), True),
        StructField("numerator_exception", BooleanType(), True),
        StructField("denominator", BooleanType(), True),
        StructField("denominator_exclusion", BooleanType(), True),
        StructField("denominator_exception", BooleanType(), True),
        StructField("initial_population", BooleanType(), True),
        StructField("from_date", DateType(), True),
        StructField("to_date", DateType(), True),
    ]
)
