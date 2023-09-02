from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct


def main():
    spark = SparkSession.builder.appName("CalcAgg").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
        .option("zkUrl", "namenode:2181").load()

    df = df.groupby("COUNTY", "PARCEL_ID")
    df = df.agg(countDistinct("OWNER_NAME").alias("DISTINCT_OWNERS"),
                countDistinct("MA_STREET_ADDRESS").alias("DISTINCT_OWNER_ADDRESS"))
    df.show(4)
    df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "ROLL_AGG_INFO") \
        .option("zkUrl", "namenode:2181") \
        .save()


main()