from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ProcessRolls").getOrCreate()
df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
    .option("zkUrl", "namenode:2181").load()

df = df.groupby("COUNTY", "SOURCE_INFO_DATE")
df = df.select("COUNTY", "SOURCE_INFO_DATE")
list = df.collect.toList

print(list)