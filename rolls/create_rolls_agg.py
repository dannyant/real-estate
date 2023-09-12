from pyspark.sql import SparkSession
from pyspark.sql.functions import count, collect_list, udf
from pyspark.sql.types import BooleanType, StringType

spark = SparkSession.builder.appName("ProcessRolls").getOrCreate()
df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
    .option("zkUrl", "namenode:2181").load()

df = df.groupby("COUNTY", "SOURCE_INFO_DATE")
df = df.agg(count("*"))
df_collect = df.collect()

def last_list_value_change(owner_list):
    if len(owner_list) > 1:
        last_owner = owner_list[-1]
        prev_owner = owner_list[-2]
        return last_owner != prev_owner
    return False


def newly_different_city(city_lst, ma_city_lst):
    if len(city_lst) >= 1 and len(ma_city_lst) > 1:
        last_city = city_lst[-1]
        last_ma_city = ma_city_lst[-1]
        prev_ma_city = ma_city_lst[-2]

        return last_city != last_ma_city and last_city == prev_ma_city
    return False


def newly_different_address(address_num_lst, address_street_lst, ma_address_lst):
    if len(address_num_lst) >= 1 and len(address_street_lst) >= 1 and len(ma_address_lst) > 1:
        last_address_num = address_num_lst[-1]
        last_address_street = address_street_lst[-1]
        last_ma_address = ma_address_lst[-1]
        prev_ma_address = ma_address_lst[-2]

        return last_address_num in prev_ma_address and last_address_street in prev_ma_address and \
            (last_address_num not in last_ma_address or last_address_street not in last_ma_address)
    return False

def get_last(array_list):
    returnVal = None
    if array_list is not None and len(array_list) > 0:
        returnVal = array_list[-1]

    return returnVal


last_list_value_change_udf = udf(last_list_value_change, BooleanType())
newly_different_city_udf = udf(newly_different_city, BooleanType())
newly_different_address_udf = udf(newly_different_address, BooleanType())
get_last_udf = udf(get_last, BooleanType())


for row in df_collect:
    row_dict = row.asDict()
    county = row_dict["COUNTY"]
    source_info_date = row_dict["SOURCE_INFO_DATE"]
    print(county + " __ " + source_info_date)

    df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
        .option("zkUrl", "namenode:2181").load()

    df = df.filter("SOURCE_INFO_DATE <= '" + source_info_date + "'")
    df = df.filter("COUNTY = '" + county + "'")

    df_groupby_parcel = df.groupby("COUNTY", "PARCEL_ID") \
        .agg(collect_list("USE_TYPE").alias("USE_TYPE_LIST"),
             collect_list("PRI_TRA").alias("PRI_TRA_LIST"),
             collect_list("SEC_TRA").alias("SEC_TRA_LIST"),
             collect_list("ADDRESS_STREET_NUM").alias("ADDRESS_STREET_NUM_LIST"),
             collect_list("ADDRESS_CITY").alias("ADDRESS_CITY_LIST"),
             collect_list("ADDRESS_UNIT_NUM").alias("ADDRESS_UNIT_NUM_LIST"),
             collect_list("ADDRESS_ZIP_EXTENSION").alias("ADDRESS_ZIP_EXTENSION_LIST"),
             collect_list("ADDRESS_ZIP").alias("ADDRESS_ZIP_LIST"),
             collect_list("TAXES_LAND_VALUE").alias("TAXES_LAND_VALUE_LIST"),
             collect_list("TAXES_IMPROVEMENT_VALUE").alias("TAXES_IMPROVEMENT_VALUE_LIST"),
             collect_list("CLCA_LAND_VALUE").alias("CLCA_LAND_VALUE_LIST"),
             collect_list("CLCA_IMPROVEMENT_VALUE").alias("CLCA_IMPROVEMENT_VALUE_LIST"),
             collect_list("FIXTURES_VALUE").alias("FIXTURES_VALUE_LIST"),
             collect_list("PERSONAL_PROPERTY_VALUE").alias("PERSONAL_PROPERTY_VALUE_LIST"),
             collect_list("HPP_VALUE").alias("HPP_VALUE_LIST"),
             collect_list("HOMEOWNERS_EXEMPTION_VALUE").alias("HOMEOWNERS_EXEMPTION_VALUE_LIST"),
             collect_list("OTHER_EXEMPTION_VALUE").alias("OTHER_EXEMPTION_VALUE_LIST"),
             collect_list("NET_TOTAL_VALUE").alias("NET_TOTAL_VALUE_LIST"),
             collect_list("LAST_DOC_PREFIX").alias("LAST_DOC_PREFIX_LIST"),
             collect_list("LAST_DOC_SERIES").alias("LAST_DOC_SERIES_LIST"),
             collect_list("LAST_DOC_DATE").alias("LAST_DOC_DATE_LIST"),
             collect_list("LAST_DOC_INPUT_DATE").alias("LAST_DOC_INPUT_DATE_LIST"),
             collect_list("OWNER_NAME").alias("OWNER_NAME_LIST"),
             collect_list("MA_CARE_OF").alias("MA_CARE_OF_LIST"),
             collect_list("MA_ATTN_NAME").alias("MA_ATTN_NAME_LIST"),
             collect_list("MA_STREET_ADDRESS").alias("MA_STREET_ADDRESS_LIST"),
             collect_list("MA_UNIT_NUMBER").alias("MA_UNIT_NUMBER_LIST"),
             collect_list("MA_CITY").alias("MA_CITY_LIST"),
             collect_list("MA_STATE").alias("MA_STATE_LIST"),
             collect_list("MA_ZIP_CODE").alias("MA_ZIP_CODE_LIST"),
             collect_list("MA_ZIP_CODE_EXTENSION").alias("MA_ZIP_CODE_EXTENSION_LIST"),
             collect_list("MA_BARECODE_WALK_SEQ").alias("MA_BARECODE_WALK_SEQ_LIST"),
             collect_list("MA_BARCODE_CHECK_DIGIT").alias("MA_BARCODE_CHECK_DIGIT_LIST"),
             collect_list("MA_EFFECTIVE_DATE").alias("MA_EFFECTIVE_DATE_LIST"),
             collect_list("MA_SOURCE_CODE").alias("MA_SOURCE_CODE_LIST"),
             collect_list("USE_CODE").alias("USE_CODE_LIST"),
             collect_list("ECON_UNIT_FLAG").alias("ECON_UNIT_FLAG_LIST"),
             collect_list("APN_INACTIVE_DATE").alias("APN_INACTIVE_DATE_LIST"),
             collect_list("ADDRESS_STREET_NAME").alias("ADDRESS_STREET_NAME_LIST")
             )

    df_groupby_parcel = df_groupby_parcel\
        .withColumn("USE_TYPE", get_last_udf(df_groupby_parcel["USE_TYPE_LIST"])) \
        .withColumn("PRI_TRA", get_last_udf(df_groupby_parcel["PRI_TRA_LIST"])) \
        .withColumn("SEC_TRA", get_last_udf(df_groupby_parcel["SEC_TRA_LIST"])) \
        .withColumn("ADDRESS_STREET_NUM", get_last_udf(df_groupby_parcel["ADDRESS_STREET_NUM_LIST"])) \
        .withColumn("ADDRESS_STREET_NAME", get_last_udf(df_groupby_parcel["ADDRESS_STREET_NAME_LIST"])) \
        .withColumn("ADDRESS_UNIT_NUM", get_last_udf(df_groupby_parcel["ADDRESS_UNIT_NUM_LIST"])) \
        .withColumn("ADDRESS_CITY", get_last_udf(df_groupby_parcel["ADDRESS_CITY_LIST"])) \
        .withColumn("ADDRESS_ZIP", get_last_udf(df_groupby_parcel["ADDRESS_ZIP_LIST"])) \
        .withColumn("ADDRESS_ZIP_EXTENSION", get_last_udf(df_groupby_parcel["ADDRESS_ZIP_EXTENSION_LIST"])) \


    def source_date():
        return source_info_date
    source_date_udf = udf(source_date, StringType())


    df_groupby_parcel = df_groupby_parcel \
        .withColumn("OWNER_NAME_CHANGE", last_list_value_change_udf(df_groupby_parcel["OWNER_NAME_LIST"])) \
        .withColumn("MA_STREET_ADDRESS_CHANGE", last_list_value_change_udf(df_groupby_parcel["MA_STREET_ADDRESS_LIST"])) \
        .withColumn("MA_CITY_CHANGE", last_list_value_change_udf(df_groupby_parcel["MA_CITY_LIST"])) \
        .withColumn("MA_STATE_CHANGE", last_list_value_change_udf(df_groupby_parcel["MA_STATE_LIST"])) \
        .withColumn("LAST_DOC_DATE_CHANGE", last_list_value_change_udf(df_groupby_parcel["LAST_DOC_DATE_LIST"])) \
        .withColumn("MA_DIFFERENT_CITY", newly_different_city_udf(df_groupby_parcel["ADDRESS_CITY_LIST"],
                                                                  df_groupby_parcel["MA_CITY_LIST"])) \
        .withColumn("MA_DIFFERENT_ADDR", newly_different_address_udf(df_groupby_parcel["ADDRESS_STREET_NUM_LIST"],
                                                                     df_groupby_parcel["ADDRESS_STREET_NAME_LIST"],
                                                                     df_groupby_parcel["MA_STREET_ADDRESS_LIST"]))


    df_groupby_parcel = df_groupby_parcel.select("COUNTY", "PARCEL_ID", "USE_TYPE", "PRI_TRA", "SEC_TRA",
                                                 "ADDRESS_STREET_NUM_LIST", "ADDRESS_STREET_NAME_LIST",
                                                 "ADDRESS_UNIT_NUM_LIST", "ADDRESS_CITY_LIST", "ADDRESS_ZIP_LIST",
                                                 "ADDRESS_ZIP_EXTENSION_LIST",
                                                 "ADDRESS_STREET_NUM", "ADDRESS_UNIT_NUM", "ADDRESS_CITY",
                                                 "ADDRESS_ZIP_EXTENSION", "ADDRESS_ZIP", "TAXES_LAND_VALUE_LIST",
                                                 "TAXES_IMPROVEMENT_VALUE_LIST", "TAXES_IMPROVEMENT_VALUE_LIST",
                                                 "CLCA_LAND_VALUE_LIST", "OWNER_NAME_CHANGE",
                                                 "CLCA_IMPROVEMENT_VALUE_LIST", "FIXTURES_VALUE_LIST",
                                                 "ADDRESS_STREET_NAME", "MA_STREET_ADDRESS_CHANGE",
                                                 "PERSONAL_PROPERTY_VALUE_LIST", "HPP_VALUE_LIST",
                                                 "HOMEOWNERS_EXEMPTION_VALUE_LIST", "MA_CITY_CHANGE",
                                                 "OTHER_EXEMPTION_VALUE_LIST", "NET_TOTAL_VALUE_LIST",
                                                 "LAST_DOC_PREFIX_LIST", "LAST_DOC_SERIES_LIST",
                                                 "LAST_DOC_DATE_LIST", "LAST_DOC_INPUT_DATE_LIST", "OWNER_NAME_LIST",
                                                 "MA_CARE_OF_LIST", "MA_STATE_CHANGE", "MA_DIFFERENT_ADDR",
                                                 "MA_ATTN_NAME_LIST", "MA_STREET_ADDRESS_LIST", "MA_UNIT_NUMBER_LIST",
                                                 "MA_CITY_LIST", "LAST_DOC_DATE_CHANGE",
                                                 "MA_STATE_LIST", "MA_ZIP_CODE_LIST", "MA_ZIP_CODE_EXTENSION_LIST",
                                                 "MA_BARECODE_WALK_SEQ_LIST", "MA_DIFFERENT_CITY",
                                                 "MA_BARCODE_CHECK_DIGIT_LIST", "MA_EFFECTIVE_DATE_LIST",
                                                 "MA_SOURCE_CODE_LIST", "USE_CODE_LIST",
                                                 "ECON_UNIT_FLAG_LIST", "APN_INACTIVE_DATE_LIST")


    df_groupby_parcel = df_groupby_parcel.withColumn("SOURCE_INFO_DATE", source_date_udf())

    df_groupby_parcel.show(5)

    df_groupby_parcel.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "ROLL_INFO_AGG") \
        .option("zkUrl", "namenode:2181") \
        .save()

