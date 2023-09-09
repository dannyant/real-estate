import traceback
from datetime import datetime

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
    elif zone[0:3] == "C-1":
        return "COMMERCIAL_LIGHT"
    elif zone[0:3] == "C-2":
        return "COMMERCIAL_SHOPPING"
    elif zone[0:3] == "C-3":
        return "COMMERCIAL_BUSINESS"
    elif zone[0:3] == "C-4":
        return "COMMERCIAL_HEAVY"
    elif zone[0:3] == "R-1":
        return "RESIDENTIAL_SF"
    elif zone[0:2] == "RD":
        return "RESIDENTIAL_UNKNOWN"
    elif zone[0:3] == "RD-":
        return "RESIDENTIAL_UNKNOWN"
    elif zone[0:3] == "RD":
        return "RESIDENTIAL_UNKNOWN"
    elif zone[0:3] == "R-2":
        return "RESIDENTIAL_MULTI_SF"
    elif zone[0:3] == "R-3":
        return "COMMERCIAL_RESIDENTIAL"
    elif zone[0:3] == "R-4":
        return "COMMERCIAL_RESIDENTIAL"
    elif zone[0:3] == "R-5":
        return "COMMERCIAL_RESIDENTIAL"
    elif zone[0:3] == "M-2":
        return "HEAVY_INDUSTRIAL"
    elif zone[0:3] == "M-1":
        return "LIGHT_INDUSTRIAL"
    elif zone[0:3] == "RMX":
        return "COMMERCIAL_MIXED"
    elif zone[0:5] == "ARP-F":
        return "FLOODPLAIN"
    else:
        return "UNKNOWN"

def reformat_sale_time(date_str):
    try:
        out = datetime.strptime(date_str, '%d-%b-%y')
        return out.strftime('%Y%m%d')
    except:
        return date_str


trimstr = udf(trim, StringType())
upperstr = udf(upper, StringType())
sacramento_udf = udf(sacramento, StringType())
create_parcel_id_udf = udf(create_parcel_id, StringType())
source_info_2023_udf = udf(source_info_2023, StringType())
zoning_udf = udf(zoning, StringType())
reformat_sale_time_udf = udf(reformat_sale_time, StringType())


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
               .withColumn("SOURCE_INFO_DATE", udfs[file]()) \
               .withColumn("LAST_DOC_DATE", reformat_sale_time_udf(df["VALUE_DT"]))

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
               .withColumnRenamed("TAX_RATE_AREA", "PRI_TRA") \
               .withColumnRenamed("LAND", "TAXES_LAND_VALUE") \
               .withColumnRenamed("IM", "TAXES_IMPROVEMENT_VALUE") \
               .withColumnRenamed("ZONING", "USE_CODE") \
               .withColumnRenamed("EX", "OTHER_EXEMPTION_VALUE") \
               .withColumnRenamed("HO_EX", "HOMEOWNERS_EXEMPTION_VALUE")

        df = df.withColumn("USE_TYPE", zoning_udf(df["USE_CODE"]))

        df2 = df.select("COUNTY", "PARCEL_ID", "SOURCE_INFO_DATE", "USE_TYPE", "USE_CODE", "ADDRESS_STREET_NUM",
                        "ADDRESS_STREET_NAME", "ADDRESS_CITY", "ADDRESS_ZIP", "OWNER_NAME", "MA_STREET_ADDRESS",
                        "MA_CITY", "MA_STATE", "MA_ZIP_CODE", "MA_CARE_OF", "LAST_DOC_DATE", "TAXES_LAND_VALUE",
                        "TAXES_IMPROVEMENT_VALUE", "PRI_TRA", "HOMEOWNERS_EXEMPTION_VALUE", "OTHER_EXEMPTION_VALUE")

        df2.write.format("org.apache.phoenix.spark") \
                .mode("overwrite") \
                .option("table", "ROLL_INFO") \
                .option("zkUrl", "namenode:2181") \
                .save()

        df3 = df.select("COUNTY", "PARCEL_ID", "USE_TYPE", "OWNER_CODE", "USE_CODE", "LAND_USE_CODE", "RECORDING_DATE", "RECORDING_PAGE", "DEED_TYPE", "FIXTURE", "PP", "EX", "ACTION_CODE")

        df3.show(200)




main()