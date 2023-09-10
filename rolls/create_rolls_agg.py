from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("ProcessRolls").getOrCreate()
df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
    .option("zkUrl", "namenode:2181").load()

df = df.groupby("COUNTY", "SOURCE_INFO_DATE")
df = df.agg(count("*"))
df_list = df.collect.toList

print(df_list)