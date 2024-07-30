from pyspark.sql import SparkSession
from deidentifier.deidentifier import DeIdentifier

columns = ["ssn", "first_name", "middle_name", "last_name", "age_column",
           "street_address_1", "eligibility_date", "death_date",
           "admission_date", "discharged_date"]
data = [(123456701, "First123456701", "", "Last123456701", 101, "Walk street",
         "2040-06-05", "2022-06-30", "2022-06-05", "2022-06-10"),
        (123456702, "First123456702", "", "Last123456702", 34, "Walk square",
         "2048-03-30", "2048-10-20", "2022-06-05", "2022-06-10"),
        (123456703, "First123456703", "Meddile123456703", "Last123456703", 88, "Walk drive, 4",
         "2050-09-01", "2051-10-20", "2022-06-05", "2022-06-10")]

spark = SparkSession.builder.appName('test_de_identification').getOrCreate()
rdd = spark.sparkContext.parallelize(data)
df = rdd.toDF(columns)
df.show()
df.printSchema()

public_df = DeIdentifier(data, df)
public_df.deid_str_addr_1()
public_df.deid_admission_discharged_dates()
public_df.deid_age(column_name='age_column')
public_df.deid_death_date()
public_df.deid_ssn_tax_id()
public_df.deid_first_name()
public_df.deid_last_name()
public_df.deid_middle_name()
public_df.remove_identified_columns()
public_df.file_df.show(truncate=False)
