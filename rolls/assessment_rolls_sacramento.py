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

def zoning(zone):
    if zone is None:
        return "UNKNOWN_NULL"
    if zone[0:3] == "C-2":
        return "COMMERCIAL_SHOPPING"
    elif zone[0:3] == "R-1":
        return "RESIDENTIAL_SF"
    elif zone[0:3] == "R-3":
        return "COMMERCIAL_RESIDENTIAL"
    elif zone[0:3] == "M-2":
        return "HEAVY_INDUSTRIAL"
    elif zone[0:3] == "M-1":
        return "LIGHT_INDUSTRIAL"
    else:
        return "UNKNOWN"


trimstr = udf(trim, StringType())
upperstr = udf(upper, StringType())
sacramento_udf = udf(sacramento, StringType())
create_parcel_id_udf = udf(create_parcel_id, StringType())
source_info_2023_udf = udf(source_info_2023, StringType())
zoning_udf = udf(zoning, StringType())


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

        df = df.withColumnRenamed("SITUS_NUMBER", "ADDRESS_STREET_NUM") \
               .withColumnRenamed("SITUS_STREET", "ADDRESS_STREET_NAME") \
               .withColumnRenamed("SITUS_STREET_SUB", "ADDRESS_CITY") \
               .withColumnRenamed("SITUS_ZIP", "ADDRESS_ZIP") \
               .withColumnRenamed("OWNER", "OWNER_NAME") \
               .withColumnRenamed("MAIL_ADDRESS", "MA_STREET_ADDRESS") \
               .withColumnRenamed("MAIL_CITY", "MA_CITY") \
               .withColumnRenamed("MAIL_STATE", "MA_STATE") \
               .withColumnRenamed("CARE_OF", "MA_CARE_OF") \
               .withColumnRenamed("MAIL_ZIP", "MA_ZIP_CODE") \
               .withColumnRenamed("ZONING", "USE_CODE")

        df = df.withColumn("USE_TYPE", zoning_udf(df["USE_CODE"]))

        df2 = df.select("COUNTY", "PARCEL_ID", "SOURCE_INFO_DATE", "USE_TYPE", "USE_CODE", "ADDRESS_STREET_NUM", "ADDRESS_STREET_NAME", "ADDRESS_CITY", "OWNER_NAME", "MA_STREET_ADDRESS", "MA_CITY", "MA_STATE", "MA_ZIP_CODE", "MA_CARE_OF")
        df2.write.format("org.apache.phoenix.spark") \
                .mode("overwrite") \
                .option("table", "ROLL_INFO") \
                .option("zkUrl", "namenode:2181") \
                .save()

        df3 = df.select("COUNTY", "PARCEL_ID", "USE_TYPE", "TAX_RATE_AREA", "OWNER_CODE", "USE_CODE", "LAND_USE_CODE", "RECORDING_DATE", "RECORDING_PAGE", "DEED_TYPE", "LAND", "IM", "FIXTURE", "PP", "HO_EX", "EX", "VALUE_DT", "ACTION_CODE")


        df2.show(50)
        df3.show(50)




main()