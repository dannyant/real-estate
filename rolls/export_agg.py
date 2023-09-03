
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("ExportAggRolls").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO_AGG") \
        .option("zkUrl", "namenode:2181").load()
    df.write.csv("hdfs://namenode:8020/user/spark/apartments/export/")

main()