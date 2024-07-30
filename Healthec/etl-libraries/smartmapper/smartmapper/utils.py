from pyspark.sql.functions import lit

# utils functions to define common business rules
# rules are applied at column level, whereas business rules are applied at
# dataframe level.
import datetime


def rule_c001(df):
    """ Adding prefix 'TEST-'"""
    return "TEST-" + df


def rule_c002(df):
    """ Convert date format from 'YYYYMMDD' to 'YYYY-MM-DD' """
    return datetime.datetime.strptime(df, '%Y%m%d').strftime('%Y-%m-%d')


def business_rule_c001(df):
    """ Adding new column 'test_col' with value 'TEST' if 'middle_initials' is part of
        column list
    """
    if "middle_initials" in df.columns:
        df = df.withColumn("test_col", lit("TEST"))
    return df
