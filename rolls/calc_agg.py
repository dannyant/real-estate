from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct


def main():
    spark = SparkSession.builder.appName("CalcAgg").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
        .option("zkUrl", "namenode:2181").load()

    df = df.groupby("COUNTY", "PARCEL_ID")
    df = df.agg(countDistinct('OWNER_NAME'))
    df = df.agg(countDistinct('MA_STREET_ADDRESS'))
    df.show(4)
    df = df.withColumnRenamed("count(OWNER_NAME)", "DISTINCT_OWNERS")
    df = df.withColumnRenamed("count(MA_STREET_ADDRESS)", "DISTINCT_ADDRESSES")
    df.show(4)

main()