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

def sacramento():
    return "SACRAMENTO"

def create_parcel_id(mapb, pg, pcl, psub):
    return mapb + pg + pcl + psub

def source_info_2023():
    return "2023"

trimstr = udf(trim, StringType())
upperstr = udf(upper, StringType())
sacramento_udf = udf(sacramento, StringType())
create_parcel_id_udf = udf(create_parcel_id, StringType())
source_info_2023_udf = udf(source_info_2023, StringType())


def main():
    spark = SparkSession.builder.appName("ProcessRolls").getOrCreate()
    separators = {"2018_secured_roll.txt" : ';', "2019_secured_roll.txt" : ';', "2020_secured_roll.txt" : ';',
                  "2021_secured_roll.txt" : ';', "2022_secured_roll.txt" : ';', "2023_secured_roll.txt" : ';'}
    udfs = {"2023_secured_roll.txt" : source_info_2023_udf}
    #for file in ["2018_secured_roll.txt", "2019_secured_roll.txt", "2020_secured_roll.txt", "2021_secured_roll.txt", "2022_secured_roll.txt", "2023_secured_roll.txt"]:
    for file in ["2023_secured_roll.txt"]:
        loc = "hdfs://namenode:8020/user/spark/apartments/rolls/sacramento/" + file
        df = spark.read.csv(loc, sep=separators[file], schema=schema)
        df = df.filter("MAPB != 'MAPB'")
        df = df.withColumn("COUNTY", sacramento_udf()) \
               .withColumn("PARCEL_ID", create_parcel_id_udf(df["MAPB"], df["PG"], df["PCL"], df["PSUB"])) \
               .withColumn("SOURCE_INFO_DATE", udfs[file]())

        df = df.withColumnRenamed("SITUS_NUMBER", "ADDRESS_STREET_NUM")\
               .withColumnRenamed("SITUS_STREET_SUB", "ADDRESS_CITY") \
               .withColumnRenamed("OWNER", "OWNER_NAME") \
               .withColumnRenamed("MAIL_ADDRESS", "MA_STREET_ADDRESS") \
               .withColumnRenamed("MAIL_CITY", "MA_CITY") \
               .withColumnRenamed("MAIL_STATE", "MA_STATE") \
               .withColumnRenamed("CARE_OF", "MA_CARE_OF") \
               .withColumnRenamed("MAIL_ZIP", "MA_ZIP_CODE")

        df2 = df.select("COUNTY", "PARCEL_ID", "SOURCE_INFO_DATE", "ADDRESS_CITY", "OWNER_NAME", "MA_STREET_ADDRESS", "MA_CITY", "MA_STATE", "MA_ZIP_CODE", "MA_CARE_OF")
        df = df.select("TAX_RATE_AREA", "SITUS_STREET", "SITUS_ZIP", "OWNER_CODE", "ZONING", "LAND_USE_CODE", "RECORDING_DATE", "RECORDING_PAGE", "DEED_TYPE", "LAND", "IM", "FIXTURE", "PP", "HO_EX", "EX", "VALUE_DT", "ACTION_CODE")


        df2.show(5)
        df.show(5)




main()