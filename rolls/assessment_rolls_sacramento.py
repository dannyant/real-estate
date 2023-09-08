import traceback

from pyspark.sql.functions import udf, collect_list
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql import SparkSession


schema = StructType([
    StructField("MAPB", StringType()),
    StructField("PG", StringType()),
    StructField("PCL", StringType()),
    StructField("PSUB", StringType()),
    StructField("TAX_RATE_AREA", StringType()),
    StructField("SITUS_NUMBER", StringType()),
    StructField("SITUS_STREET_SUB", StringType()),
    StructField("SITUS_STREET", StringType()),
    StructField("SITUS_ZIP", StringType()),
    StructField("OWNER_CODE", StringType()),
    StructField("OWNER", StringType()),
    StructField("MAIL_ADDRESS", StringType()),
    StructField("MAIL_CITY", StringType()),
    StructField("MAIL_STATE", StringType()),
    StructField("MAIL_ZIP", StringType()),
    StructField("CARE_OF", StringType()),
    StructField("ZONING", StringType()),
    StructField("LAND_USE_CODE", StringType()),
    StructField("RECORDING_DATE", StringType()),
    StructField("RECORDING_PAGE", StringType()),
    StructField("DEED_TYPE", StringType()),
    StructField("LAND", StringType()),
    StructField("IM", StringType()),
    StructField("FIXTURE", StringType()),
    StructField("PP", StringType()),
    StructField("HO_EX", StringType()),
    StructField("EX", StringType()),
    StructField("VALUE_DT", StringType()),
    StructField("ACTION_CODE", StringType())
])

def trim(val):
    return val.strip()

def upper(val):
    return val.upper().strip()

def sacramento(val):
    return "SACRAMENTO"

trimstr = udf(trim, StringType())
upperstr = udf(upper, StringType())
sacramento_udf = udf(sacramento, StringType())


def main():
    spark = SparkSession.builder.appName("ProcessRolls").getOrCreate()
    separators = {"2018_secured_roll.txt" : ';', "2019_secured_roll.txt" : ';', "2020_secured_roll.txt" : ';',
                  "2021_secured_roll.txt" : ';', "2022_secured_roll.txt" : ';', "2023_secured_roll.txt" : ';'}
    #for file in ["2018_secured_roll.txt", "2019_secured_roll.txt", "2020_secured_roll.txt", "2021_secured_roll.txt", "2022_secured_roll.txt", "2023_secured_roll.txt"]:
    for file in ["2023_secured_roll.txt"]:
        loc = "hdfs://namenode:8020/user/spark/apartments/rolls/sacramento/" + file
        df = spark.read.csv(loc, sep=separators[file], schema=schema)
        df = df.withColumn("COUNTY", sacramento_udf())
        df.show(5)



main()