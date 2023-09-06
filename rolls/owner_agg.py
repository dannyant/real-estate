from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

spark = SparkSession.builder.appName("ProcessRolls").getOrCreate()
df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
    .option("zkUrl", "namenode:2181").load()

owner_df = df.select("COUNTY", "SOURCE_INFO_DATE", "OWNER_NAME", "MA_STREET_ADDRESS", "MA_CITY", "MA_STATE", "MA_UNIT_NUMBER",
                           "MA_ZIP_CODE", "MA_ZIP_CODE_EXTENSION", "CARE_OF", "ATTN_NAME", "PARCEL_ID")

owner_df = owner_df.withColumnRenamed("MA_STREET_ADDRESS", "STREET_ADDRESS") \
                   .withColumnRenamed("MA_CITY", "CITY") \
                   .withColumnRenamed("MA_UNIT_NUMBER", "UNIT_NUMBER") \
                   .withColumnRenamed("MA_ZIPCODE", "ZIPCODE") \
                   .withColumnRenamed("MA_ZIPCODE_EXTENSION", "ZIPCODE_EXTENSION") \
                   .withColumnRenamed("MA_STATE", "STATE") \

owner_df = owner_df.groupBy("COUNTY", "SOURCE_INFO_DATE", "OWNER_NAME", "STREET_ADDRESS", "CITY", "STATE", "UNIT_NUMBER",
                           "ZIPCODE", "ZIPCODE_EXTENSION", "CARE_OF", "ATTN_NAME")

owner_df = owner_df.agg(collect_list("PARCEL_ID").alias("OWNED_PROPERTIES"))

owner_df.write.format("org.apache.phoenix.spark") \
    .mode("overwrite") \
    .option("table", "OWNER_INFO") \
    .option("zkUrl", "namenode:2181") \
    .save()

