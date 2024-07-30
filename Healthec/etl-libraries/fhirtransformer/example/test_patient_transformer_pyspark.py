from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import SparkSession

transfomer = FHIRTransformer()
transfomer.load_template("patient.j2")

spark = SparkSession.builder.appName("fhir-transform-app").getOrCreate()
data_file_path = "example/data/patient.csv"
patient_df = spark.read.options(header=True).csv(data_file_path)
patient_df.show(truncate=False)

patient_fhir_df = transfomer.render_resource_df("resource", "Patient", patient_df)

patient_fhir_df.show(truncate=False)
