from pyspark.sql.functions import lit

# utils functions to define business rules
import datetime

def rule_c003(df):
    """ Adding custom prefix"""
    return "USER-" + df

def business_rule_c002(df):
    """ Adding format type """
    if "middle_initials" in df.columns:
        df = df.withColumn("user_format_type", lit("USR01"))
    return df
