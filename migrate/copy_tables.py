from pyspark.sql import SparkSession


tables = ["ADDRESS_INFO", "APARTMENTS_PROPERTY", "OWNER_INFO", "OWNER_NAME_PROPERTY_COUNT", "PARCEL_INFO",
          "ROLL_AGG_INFO", "ROLL_INFO", "ROLL_INFO_AGG", "TAX_INFO", "TAX_INFO_STATUS", "VALUE_INFO"]

for t in tables:
    spark = SparkSession.builder.appName("CopyTables").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", t) \
        .option("zkUrl", "namenode:2181").load()

    df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "ROLL_INFO_AGG") \
        .option("zkUrl", "192.168.108:2181") \
        .save()
