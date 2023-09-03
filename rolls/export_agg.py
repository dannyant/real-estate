
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = SparkSession.builder.appName("ExportAggRolls").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO_AGG") \
        .option("zkUrl", "namenode:2181").load()
    df = df.withColumn("USE_TYPE_LIST", col("USE_TYPE_LIST").cast("string")) \
        .withColumn("PRI_TRA_LIST", col("PRI_TRA_LIST").cast("string")) \
        .withColumn("SEC_TRA_LIST", col("SEC_TRA_LIST").cast("string")) \
        .withColumn("ADDRESS_STREET_NUM_LIST", col("ADDRESS_STREET_NUM_LIST").cast("string")) \
        .withColumn("ADDRESS_STREET_NAME_LIST", col("ADDRESS_STREET_NAME_LIST").cast("string")) \
        .withColumn("ADDRESS_UNIT_NUM_LIST", col("ADDRESS_UNIT_NUM_LIST").cast("string")) \
        .withColumn("ADDRESS_CITY_LIST", col("ADDRESS_CITY_LIST").cast("string")) \
        .withColumn("ADDRESS_ZIP_LIST", col("ADDRESS_ZIP_LIST").cast("string")) \
        .withColumn("ADDRESS_ZIP_EXTENSION_LIST", col("ADDRESS_ZIP_EXTENSION_LIST").cast("string")) \
        .withColumn("TAXES_LAND_VALUE_LIST", col("TAXES_LAND_VALUE_LIST").cast("string")) \
        .withColumn("TAXES_IMPROVEMENT_VALUE_LIST", col("TAXES_IMPROVEMENT_VALUE_LIST").cast("string")) \
        .withColumn("CLCA_LAND_VALUE_LIST", col("CLCA_LAND_VALUE_LIST").cast("string")) \
        .withColumn("CLCA_IMPROVEMENT_VALUE_LIST", col("CLCA_IMPROVEMENT_VALUE_LIST").cast("string")) \
        .withColumn("FIXTURES_VALUE_LIST", col("FIXTURES_VALUE_LIST").cast("string")) \
        .withColumn("PERSONAL_PROPERTY_VALUE_LIST", col("PERSONAL_PROPERTY_VALUE_LIST").cast("string")) \
        .withColumn("HPP_VALUE_LIST", col("HPP_VALUE_LIST").cast("string")) \
        .withColumn("HOMEOWNERS_EXEMPTION_VALUE_LIST", col("HOMEOWNERS_EXEMPTION_VALUE_LIST").cast("string")) \
        .withColumn("OTHER_EXEMPTION_VALUE_LIST", col("OTHER_EXEMPTION_VALUE_LIST").cast("string")) \
        .withColumn("NET_TOTAL_VALUE_LIST", col("NET_TOTAL_VALUE_LIST").cast("string")) \
        .withColumn("LAST_DOC_PREFIX_LIST", col("LAST_DOC_PREFIX_LIST").cast("string")) \
        .withColumn("LAST_DOC_SERIES_LIST", col("LAST_DOC_SERIES_LIST").cast("string")) \
        .withColumn("LAST_DOC_SERIES_LIST", col("LAST_DOC_SERIES_LIST").cast("string")) \
        .withColumn("LAST_DOC_DATE_LIST", col("LAST_DOC_DATE_LIST").cast("string")) \
        .withColumn("LAST_DOC_INPUT_DATE_LIST", col("LAST_DOC_INPUT_DATE_LIST").cast("string")) \
        .withColumn("OWNER_NAME_LIST", col("OWNER_NAME_LIST").cast("string")) \
        .withColumn("OWNER_NAME_LIST", col("OWNER_NAME_LIST").cast("string")) \
        .withColumn("MA_CARE_OF_LIST", col("MA_CARE_OF_LIST").cast("string")) \
        .withColumn("MA_ATTN_NAME_LIST", col("MA_ATTN_NAME_LIST").cast("string")) \
        .withColumn("MA_STREET_ADDRESS_LIST", col("MA_STREET_ADDRESS_LIST").cast("string")) \
        .withColumn("MA_UNIT_NUMBER_LIST", col("MA_UNIT_NUMBER_LIST").cast("string")) \
        .withColumn("MA_CITY_STATE_LIST", col("MA_CITY_STATE_LIST").cast("string")) \
        .withColumn("MA_ZIP_CODE_LIST", col("MA_ZIP_CODE_LIST").cast("string")) \
        .withColumn("MA_ZIP_CODE_EXTENSION_LIST", col("MA_ZIP_CODE_EXTENSION_LIST").cast("string")) \
        .withColumn("MA_BARECODE_WALK_SEQ_LIST", col("MA_BARECODE_WALK_SEQ_LIST").cast("string")) \
        .withColumn("MA_BARCODE_CHECK_DIGIT_LIST", col("MA_BARCODE_CHECK_DIGIT_LIST").cast("string")) \
        .withColumn("MA_EFFECTIVE_DATE_LIST", col("MA_EFFECTIVE_DATE_LIST").cast("string")) \
        .withColumn("MA_SOURCE_CODE_LIST", col("MA_SOURCE_CODE_LIST").cast("string")) \
        .withColumn("USE_CODE_LIST", col("USE_CODE_LIST").cast("string")) \
        .withColumn("ECON_UNIT_FLAG_LIST", col("ECON_UNIT_FLAG_LIST").cast("string")) \
        .withColumn("APN_INACTIVE_DATE_LIST", col("APN_INACTIVE_DATE_LIST").cast("string"))


    df.write.csv("hdfs://namenode:8020/user/spark/apartments/export/export.csv")

main()