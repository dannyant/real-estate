from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

spark = SparkSession.builder.appName("ProcessRolls").getOrCreate()
df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
    .option("zkUrl", "namenode:2181").load()

def owner_lives_at_address(address_num, address_street, ma_address):
    return address_num in ma_address and address_street in ma_address

owner_lives_at_address_udf = udf(owner_lives_at_address, BooleanType())

df = df.withColumn("OWNER_LIVES_IN_PROPERTY", owner_lives_at_address_udf(df["ADDRESS_STREET_NUM"],
                                                                         df["ADDRESS_STREET_NAME"],
                                                                         df["MA_STREET_ADDRESS"]))

df = df.select("COUNTY", "PARCEL_ID", "SOURCE_INFO_DATE", "OWNER_LIVES_IN_PROPERTY")

df.write.format("org.apache.phoenix.spark").mode("overwrite") \
    .option("table", "ROLL_INFO").option("zkUrl", "namenode:2181").save()
