from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from assessment_rolls import use_code_type

def main():
    spark = SparkSession.builder.appName("CalcAgg").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
        .option("zkUrl", "namenode:2181").load()
    df = df.withColumn("USE_TYPE", use_code_type(df["USE_CODE"]))

    df_groupby_parcel = df.groupby("COUNTY", "PARCEL_ID")
    df_groupby_parcel.show(1)

    df_groupby_owner = df.select("OWNER_NAME", "MA_STREET_ADDRESS", "MA_CITY_STATE", "PARCEL_ID")
    df_groupby_owner = df_groupby_owner.groupby("OWNER_NAME", "MA_STREET_ADDRESS", "MA_CITY_STATE")
    df_groupby_owner = df_groupby_owner.agg(countDistinct("PARCEL_ID").alias("PARCEL_COUNT"))
    df_groupby_owner.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "OWNER_NAME_PROPERTY_COUNT") \
        .option("zkUrl", "namenode:2181") \
        .save()


    df_groupby_use_type = df.select("COUNTY", "PARCEL_ID", "USE_TYPE", "OWNER_NAME", "MA_STREET_ADDRESS")

    df_groupby_use_type = df_groupby_use_type.groupby("COUNTY", "PARCEL_ID", "USE_TYPE")
    df_groupby_use_type = df_groupby_use_type.agg(countDistinct("OWNER_NAME").alias("DISTINCT_OWNERS"),
                countDistinct("MA_STREET_ADDRESS").alias("DISTINCT_OWNER_ADDRESS"))
    df_groupby_use_type.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "ROLL_AGG_INFO") \
        .option("zkUrl", "namenode:2181") \
        .save()


main()