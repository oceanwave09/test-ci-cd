from pyspark.sql import SparkSession

from smartmapper.mapper import SmartMapper

spark = SparkSession.builder.appName('data-pipeline-job').getOrCreate()

SCHEMA_FILE = "schema/sample-mapper-for-flatfile.yaml"
SRC_FILE = "data/sample-data-file.txt"

# SCHEMA_FILE = "s3://hec-sandbox-data-pipeline-bucket/smartmapper/mapper/sample-mapper-for-flatfile.yaml"
# SRC_FILE = "data/sample-data-file.txt"

# SCHEMA_FILE = "schema/sample-mapper-for-delimiter-with-src.yaml"
# SRC_FILE = "data/sample-data-file.csv"

# SCHEMA_FILE = "schema/sample-mapper-for-delimiter-with-pos.yaml"
# SRC_FILE = "data/sample-data-file.csv"

mapper = SmartMapper.new_mapper()
mapper.set_schema(SCHEMA_FILE)
mapper.load_rules("user_rules.py")
print(mapper.schema)
print(mapper.rules)

# read the source file
if mapper.schema.is_delimited_file:
    src_df = spark.read \
        .options(delimiter=mapper.schema.delimiter, header=mapper.schema.has_header) \
        .csv(SRC_FILE)
else:
    src_df = spark.read.text(SRC_FILE)
src_df.show(1, truncate=False)

transform_df = src_df.transform(mapper.map_df)
transform_df.printSchema()
transform_df.show(1, truncate=False)
