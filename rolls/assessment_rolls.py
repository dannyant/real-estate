import traceback

from pyspark.sql.functions import udf, collect_list
from pyspark.sql.types import StructType, StructField, StringType, NumericType, IntegerType, BooleanType
from pyspark.sql import SparkSession

column_translations = {
    "Assessor's Parcel Number (APN) sort format": "APN_LONG",
    "APN print format": "APN_SHORT",
    "Primary tax rate area (TRA)": "PRI_TRA",
    "Secondary TRA": "SEC_TRA",
    "Situs street number": "ADDRESS_STREET_NUM",
    "Situs street name": "ADDRESS_STREET_NAME",
    "Situs unit number": "ADDRESS_UNIT_NUM",
    "Situs city": "ADDRESS_CITY",
    "Situs zip code": "ADDRESS_ZIP",
    "Situs zip+4": "ADDRESS_ZIP_EXTENSION",
    "Land value": "TAXES_LAND_VALUE",
    "Improvements value": "TAXES_IMPROVEMENT_VALUE",
    "CLCA land value": "CLCA_LAND_VALUE",
    "CLCA improvements value": "CLCA_IMPROVEMENT_VALUE",
    "Fixtures value": "FIXTURES_VALUE",
    "Personal property value": "PERSONAL_PROPERTY_VALUE",
    "Household personal property (HPP) value": "HPP_VALUE",
    "Homeowners' exemption value": "HOMEOWNERS_EXEMPTION_VALUE",
    "Other exemption value": "OTHER_EXEMPTION_VALUE",
    "Net total value": "NET_TOTAL_VALUE",
    "Last document prefix": "LAST_DOC_PREFIX",
    "Last document series": "LAST_DOC_SERIES",
    "Last document date (CCYYMMDD)": "LAST_DOC_DATE",
    "Last document input date (CCYYMMDD)": "LAST_DOC_INPUT_DATE",
    "Owner name": "OWNER_NAME",
    "Mailing address (MA) care of name": "MA_CARE_OF",
    "MA attention name": "MA_ATTN_NAME",
    "MA street address": "MA_STREET_ADDRESS",
    "MA unit number": "MA_UNIT_NUMBER",
    "MA city and state": "MA_CITY_STATE",
    "MA zip code": "MA_ZIP_CODE",
    "MA zip+4": "MA_ZIP_CODE_EXTENSION",
    "MA barcode walk sequence": "MA_BARECODE_WALK_SEQ",
    "MA barcode check digit": "MA_BARCODE_CHECK_DIGIT",
    "MA effective date (CCYYMMDD)": "MA_EFFECTIVE_DATE",
    "MA source code": "MA_SOURCE_CODE",
    "Use code": "USE_CODE",
    "Economic unit flag 'Y' if APN is part of an economic unit": "ECON_UNIT_FLAG",
    "End date (CCYYMMDD) Date APN was inactivated Blank if APN is active": "APN_INACTIVE_DATE"
}

schema = StructType([
    StructField("APN_LONG", StringType()),
    StructField("APN_SHORT", StringType()),
    StructField("PRI_TRA", StringType()),
    StructField("SEC_TRA", StringType()),
    StructField("ADDRESS_STREET_NUM", StringType()),
    StructField("ADDRESS_STREET_NAME", StringType()),
    StructField("ADDRESS_UNIT_NUM", StringType()),
    StructField("ADDRESS_CITY", StringType()),
    StructField("ADDRESS_ZIP", StringType()),
    StructField("ADDRESS_ZIP_EXTENSION", StringType()),
    StructField("TAXES_LAND_VALUE", StringType()),
    StructField("TAXES_IMPROVEMENT_VALUE", StringType()),
    StructField("CLCA_LAND_VALUE", StringType()),
    StructField("CLCA_IMPROVEMENT_VALUE", StringType()),
    StructField("FIXTURES_VALUE", StringType()),
    StructField("PERSONAL_PROPERTY_VALUE", StringType()),
    StructField("HPP_VALUE", StringType()),
    StructField("HOMEOWNERS_EXEMPTION_VALUE", StringType()),
    StructField("OTHER_EXEMPTION_VALUE", StringType()),
    StructField("NET_TOTAL_VALUE", StringType()),
    StructField("LAST_DOC_PREFIX", StringType()),
    StructField("LAST_DOC_SERIES", StringType()),
    StructField("LAST_DOC_DATE", StringType()),
    StructField("LAST_DOC_INPUT_DATE", StringType()),
    StructField("OWNER_NAME", StringType()),
    StructField("MA_CARE_OF", StringType()),
    StructField("MA_ATTN_NAME", StringType()),
    StructField("MA_STREET_ADDRESS", StringType()),
    StructField("MA_UNIT_NUMBER", StringType()),
    StructField("MA_CITY_STATE", StringType()),
    StructField("MA_ZIP_CODE", StringType()),
    StructField("MA_ZIP_CODE_EXTENSION", StringType()),
    StructField("MA_BARECODE_WALK_SEQ", StringType()),
    StructField("MA_BARCODE_CHECK_DIGIT", StringType()),
    StructField("MA_EFFECTIVE_DATE", StringType()),
    StructField("MA_SOURCE_CODE", StringType()),
    StructField("USE_CODE", StringType()),
    StructField("ECON_UNIT_FLAG", StringType()),
    StructField("APN_INACTIVE_DATE", StringType())
])


def get_use_code_type(use_code_str):
    try:
        use_code = int(use_code_str)
        if use_code < 1000:
            return "EXEMPT"
        elif use_code < 2000:
            return "RESIDENTIAL_SF"
        elif use_code < 3000:
            return "RESIDENTIAL_MF"
        elif use_code < 4000:
            return "COMMERCIAL_SHOPPING"
        elif use_code < 5000:
            return "INDUSTRIAL"
        elif use_code < 6000:
            return "RURAL"
        elif use_code < 7000:
            return "INSTITUTIONAL"
        elif 7300 <= use_code <= 7430:
            return "COMMERCIAL_CONDO"
        elif use_code <= 8000:
            return "COMMERCIAL_RESIDENTIAL"
        elif use_code <= 9000:
            return "COMMERCIAL_IMPROVED"
        elif use_code <= 9000:
            return "COMMERCIAL_IMPROVED"
        else:
            return "COMMERCIAL_MISC"
    except:
        return "UNKOWN"

def alameda():
    return "ALAMEDA"

def literal_nov17():
    return "2017-11-01"

def literal_may22():
    return "2022-05-01"

def literal_aug23():
    return "2023-08-01"

def trim(val):
    return val.strip()

def upper(val):
    return val.upper().strip()

def is_owner_change(owner_list):
    if len(owner_list) > 1:
        last_owner = owner_list[-1]
        prev_owner = owner_list[-2]
        return last_owner != prev_owner
    return False

def is_street_change(street_list):
    if len(street_list) > 1:
        last_street = street_list[-1]
        prev_street = street_list[-2]
        return last_street != prev_street
    return False

def is_city_change(city_list):
    if len(city_list) > 1:
        last_city = city_list[-1]
        prev_city = city_list[-2]
        return last_city != prev_city
    return False

def is_state_change(state_list):
    if len(state_list) > 1:
        last_city = state_list[-1]
        prev_city = state_list[-2]
        return last_city != prev_city
    return False

def split_address_get_state(val):
    val = val.strip()
    split = val.split(" ")
    print("State = " + val + " -- " + str(split))
    return split[-1]

def split_address_get_city(val):
    val = val.strip()
    split = val.split(" ")
    print("City = " + val + " -- " + str(split))
    state = split[-1]
    return val[0:-len(state)].strip()

def to_int(val):
    try:
        if val is None:
            return None
        return int(val.strip())
    except:
        traceback.print_exc()
        return None


alameda_udf = udf(alameda, StringType())
literal_nov17_udf = udf(literal_nov17, StringType())
literal_may22_udf = udf(literal_may22, StringType())
literal_aug23_udf = udf(literal_aug23, StringType())

trimstr = udf(trim, StringType())
upperstr = udf(upper, StringType())
use_code_type = udf(get_use_code_type, StringType())
split_state = udf(split_address_get_state, StringType())
split_city = udf(split_address_get_city, StringType())
to_int_conv = udf(to_int, IntegerType())

is_owner_change_udf = udf(is_owner_change, BooleanType())
is_street_change_udf = udf(is_street_change, BooleanType())
is_city_change_udf = udf(is_city_change, BooleanType())
is_state_change_udf = udf(is_state_change, BooleanType())

def main_old():
    spark = SparkSession.builder.appName("ProcessRolls").getOrCreate()
    df = spark.read.csv("hdfs://namenode:8020/user/spark/apartments/rolls", sep="\t", schema=schema)
    df = df.withColumn("COUNTY", alameda_udf())\
        .withColumn("PARCEL_ID", trimstr(df["APN_SHORT"])) \
        .withColumn("USE_TYPE", use_code_type(df["USE_CODE"])) \
        .withColumn("STREET_NUM", upperstr(df["ADDRESS_STREET_NUM"])) \
        .withColumn("UNIT_NUM", upperstr(df["ADDRESS_UNIT_NUM"])) \
        .withColumn("STREET_NAME", upperstr(df["ADDRESS_STREET_NAME"])) \
        .withColumn("CITY", upperstr(df["ADDRESS_CITY"])) \
        .withColumn("ZIP", upperstr(df["ADDRESS_ZIP"])) \
        .withColumn("ZIP_EXTENSION", upperstr(df["ADDRESS_ZIP_EXTENSION"])) \
        .withColumn("USE_TYPE", use_code_type(df["USE_CODE"]))


    apn_df = df.select("PARCEL_ID", "COUNTY")
    apn_df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "PARCEL_INFO") \
        .option("zkUrl", "namenode:2181") \
        .save()

    tax_df = df.select("PARCEL_ID", "COUNTY", "USE_TYPE")
    tax_df = tax_df.filter("USE_TYPE = 'COMMERCIAL_RESIDENTIAL'")
    tax_df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "tax_info") \
        .option("zkUrl", "namenode:2181") \
        .save()

    address_df = df.withColumn("STREET_NUMBER", upperstr(df["ADDRESS_STREET_NUM"])) \
        .withColumn("UNIT_NUMBER", upperstr(df["ADDRESS_UNIT_NUM"])) \
        .withColumn("STREET_NAME", upperstr(df["ADDRESS_STREET_NAME"])) \
        .withColumn("CITY", upperstr(df["ADDRESS_CITY"])) \
        .withColumn("ZIPCODE", upperstr(df["ADDRESS_ZIP"])) \
        .withColumn("ZIPCODE_EXTENSION", upperstr(df["ADDRESS_ZIP_EXTENSION"])) \
        .withColumn("USE_TYPE", use_code_type(df["USE_CODE"]))

    address_df = address_df.select("PARCEL_ID", "COUNTY", "STREET_NUMBER", "UNIT_NUMBER", "STREET_NAME",
                           "CITY", "ZIPCODE", "ZIPCODE_EXTENSION", "USE_TYPE")
    address_df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "ADDRESS_INFO") \
        .option("zkUrl", "namenode:2181") \
        .save()

    owner_df = df.withColumn("OWNER_NAME", upperstr(df["OWNER_NAME"])) \
        .withColumn("STREET_ADDRESS", upperstr(df["MA_STREET_ADDRESS"])) \
        .withColumn("UNIT_NUMBER", upperstr(df["MA_UNIT_NUMBER"])) \
        .withColumn("ZIPCODE", upperstr(df["MA_ZIP_CODE"])) \
        .withColumn("ZIPCODE_EXTENSION", upperstr(df["MA_ZIP_CODE_EXTENSION"])) \
        .withColumn("CARE_OF", upperstr(df["MA_CARE_OF"])) \
        .withColumn("CITY", split_city(df["MA_CITY_STATE"])) \
        .withColumn("STATE", split_state(df["MA_CITY_STATE"])) \
        .withColumn("ATTN_NAME", upperstr(df["MA_ATTN_NAME"]))

    owner_df = owner_df.select("OWNER_NAME", "STREET_ADDRESS", "CITY", "STATE", "UNIT_NUMBER",
                         "ZIPCODE", "ZIPCODE_EXTENSION", "CARE_OF", "ATTN_NAME")
    owner_df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "OWNER_INFO") \
        .option("zkUrl", "namenode:2181") \
        .save()

    value_df = df.select("COUNTY", "PARCEL_ID", "TAXES_LAND_VALUE", "TAXES_IMPROVEMENT_VALUE",
                         "CLCA_LAND_VALUE", "CLCA_IMPROVEMENT_VALUE", "FIXTURES_VALUE", "PERSONAL_PROPERTY_VALUE",
                         "HPP_VALUE", "HOMEOWNERS_EXEMPTION_VALUE", "OTHER_EXEMPTION_VALUE", "NET_TOTAL_VALUE",
                         "LAST_DOC_PREFIX", "LAST_DOC_SERIES", "LAST_DOC_DATE", "LAST_DOC_INPUT_DATE")

    value_df = value_df.withColumn("TAXES_LAND_VALUE", to_int_conv(df["TAXES_LAND_VALUE"])) \
        .withColumn("TAXES_IMPROVEMENT_VALUE", to_int_conv(df["TAXES_IMPROVEMENT_VALUE"])) \
        .withColumn("CLCA_LAND_VALUE", to_int_conv(df["CLCA_LAND_VALUE"])) \
        .withColumn("CLCA_IMPROVEMENT_VALUE", to_int_conv(df["CLCA_IMPROVEMENT_VALUE"])) \
        .withColumn("FIXTURES_VALUE", to_int_conv(df["FIXTURES_VALUE"])) \
        .withColumn("PERSONAL_PROPERTY_VALUE", to_int_conv(df["PERSONAL_PROPERTY_VALUE"])) \
        .withColumn("HPP_VALUE", to_int_conv(df["HPP_VALUE"])) \
        .withColumn("HOMEOWNERS_EXEMPTION_VALUE", to_int_conv(df["HOMEOWNERS_EXEMPTION_VALUE"])) \
        .withColumn("OTHER_EXEMPTION_VALUE", to_int_conv(df["OTHER_EXEMPTION_VALUE"])) \
        .withColumn("NET_TOTAL_VALUE", to_int_conv(df["NET_TOTAL_VALUE"]))


    value_df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "VALUE_INFO") \
        .option("zkUrl", "namenode:2181") \
        .save()

def main():
    spark = SparkSession.builder.appName("ProcessRolls").getOrCreate()
    file_map = {"IE670-11-01-17.TXT" : literal_nov17_udf, "IE670-05-01-22.TXT" : literal_may22_udf, "IE670-08-01-23.TXT" : literal_aug23_udf}
    for file in ["IE670-11-01-17.TXT", "IE670-05-01-22.TXT", "IE670-08-01-23.TXT"]:
        loc = "hdfs://namenode:8020/user/spark/apartments/rolls/" + file
        df = spark.read.csv(loc, sep="\t", schema=schema)
        df = df.withColumn("COUNTY", alameda_udf())\
            .withColumn("SOURCE_INFO_DATE", file_map[file]()) \
            .withColumn("PARCEL_ID", trimstr(df["APN_SHORT"])) \
            .withColumn("USE_TYPE", use_code_type(df["USE_CODE"])) \
            .withColumn("COUNTY", alameda_udf())\
            .withColumn("PARCEL_ID", trimstr(df["APN_SHORT"])) \
            .withColumn("ADDRESS_STREET_NAME", trimstr(df["ADDRESS_STREET_NAME"])) \
            .withColumn("ADDRESS_UNIT_NUM", trimstr(df["ADDRESS_UNIT_NUM"])) \
            .withColumn("ADDRESS_CITY", trimstr(df["ADDRESS_CITY"])) \
            .withColumn("ADDRESS_ZIP", trimstr(df["ADDRESS_ZIP"])) \
            .withColumn("ADDRESS_ZIP_EXTENSION", trimstr(df["ADDRESS_ZIP_EXTENSION"])) \
            .withColumn("TAXES_LAND_VALUE", to_int_conv(df["TAXES_LAND_VALUE"])) \
            .withColumn("TAXES_IMPROVEMENT_VALUE", to_int_conv(df["TAXES_IMPROVEMENT_VALUE"])) \
            .withColumn("CLCA_LAND_VALUE", to_int_conv(df["CLCA_LAND_VALUE"])) \
            .withColumn("CLCA_IMPROVEMENT_VALUE", to_int_conv(df["CLCA_IMPROVEMENT_VALUE"])) \
            .withColumn("FIXTURES_VALUE", to_int_conv(df["FIXTURES_VALUE"])) \
            .withColumn("PERSONAL_PROPERTY_VALUE", to_int_conv(df["PERSONAL_PROPERTY_VALUE"])) \
            .withColumn("HPP_VALUE", to_int_conv(df["HPP_VALUE"])) \
            .withColumn("HOMEOWNERS_EXEMPTION_VALUE", to_int_conv(df["HOMEOWNERS_EXEMPTION_VALUE"])) \
            .withColumn("OTHER_EXEMPTION_VALUE", to_int_conv(df["OTHER_EXEMPTION_VALUE"])) \
            .withColumn("LAST_DOC_PREFIX", trimstr(df["LAST_DOC_PREFIX"])) \
            .withColumn("LAST_DOC_SERIES", trimstr(df["LAST_DOC_SERIES"])) \
            .withColumn("LAST_DOC_DATE", trimstr(df["LAST_DOC_DATE"])) \
            .withColumn("LAST_DOC_INPUT_DATE", trimstr(df["LAST_DOC_INPUT_DATE"])) \
            .withColumn("OWNER_NAME", trimstr(df["OWNER_NAME"])) \
            .withColumn("MA_CARE_OF", trimstr(df["MA_CARE_OF"])) \
            .withColumn("MA_ATTN_NAME", trimstr(df["MA_ATTN_NAME"])) \
            .withColumn("MA_STREET_ADDRESS", trimstr(df["MA_STREET_ADDRESS"])) \
            .withColumn("MA_UNIT_NUMBER", trimstr(df["MA_UNIT_NUMBER"])) \
            .withColumn("MA_CITY", split_city(df["MA_CITY_STATE"])) \
            .withColumn("MA_STATE", split_state(df["MA_CITY_STATE"])) \
            .withColumn("MA_ZIP_CODE", trimstr(df["MA_ZIP_CODE"])) \
            .withColumn("MA_ZIP_CODE_EXTENSION", trimstr(df["MA_ZIP_CODE_EXTENSION"])) \
            .withColumn("MA_BARECODE_WALK_SEQ", trimstr(df["MA_BARECODE_WALK_SEQ"])) \
            .withColumn("MA_BARCODE_CHECK_DIGIT", trimstr(df["MA_BARCODE_CHECK_DIGIT"])) \
            .withColumn("MA_EFFECTIVE_DATE", trimstr(df["MA_EFFECTIVE_DATE"])) \
            .withColumn("MA_SOURCE_CODE", trimstr(df["MA_SOURCE_CODE"])) \
            .withColumn("USE_CODE", trimstr(df["USE_CODE"])) \
            .withColumn("ECON_UNIT_FLAG", trimstr(df["ECON_UNIT_FLAG"])) \
            .withColumn("APN_INACTIVE_DATE", trimstr(df["APN_INACTIVE_DATE"])) \
            .withColumn("NET_TOTAL_VALUE", to_int_conv(df["NET_TOTAL_VALUE"]))


        df = df.select("COUNTY", "PARCEL_ID", "USE_TYPE", "SOURCE_INFO_DATE", "PRI_TRA", "SEC_TRA", "ADDRESS_STREET_NUM",
                       "ADDRESS_STREET_NAME", "ADDRESS_UNIT_NUM", "ADDRESS_CITY", "ADDRESS_ZIP", "ADDRESS_ZIP_EXTENSION",
                        "TAXES_LAND_VALUE", "TAXES_IMPROVEMENT_VALUE", "CLCA_LAND_VALUE", "CLCA_IMPROVEMENT_VALUE",
                       "FIXTURES_VALUE", "PERSONAL_PROPERTY_VALUE", "HPP_VALUE", "HOMEOWNERS_EXEMPTION_VALUE",
                       "OTHER_EXEMPTION_VALUE", "NET_TOTAL_VALUE", "LAST_DOC_PREFIX", "LAST_DOC_SERIES", "LAST_DOC_DATE",
                       "LAST_DOC_INPUT_DATE", "OWNER_NAME", "MA_CARE_OF", "MA_ATTN_NAME", "MA_STREET_ADDRESS",
                       "MA_UNIT_NUMBER", "MA_CITY", "MA_STATE", "MA_ZIP_CODE", "MA_ZIP_CODE_EXTENSION", "MA_BARECODE_WALK_SEQ",
                       "MA_BARCODE_CHECK_DIGIT", "MA_EFFECTIVE_DATE", "MA_SOURCE_CODE", "USE_CODE", "ECON_UNIT_FLAG",
                        "APN_INACTIVE_DATE")\
            .withColumn("ADDRESS_STREET_NUM", upperstr(df["ADDRESS_STREET_NUM"]))


        df.write.format("org.apache.phoenix.spark") \
                .mode("overwrite") \
                .option("table", "ROLL_INFO") \
                .option("zkUrl", "namenode:2181") \
                .save()

        df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
            .option("zkUrl", "namenode:2181").load()
        df = df.withColumn("USE_TYPE", use_code_type(df["USE_CODE"]))

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

        df_groupby_parcel = df_groupby_parcel.select("COUNTY", "PARCEL_ID", "SOURCE_INFO_DATE", "USE_TYPE_LIST",
                    "PRI_TRA_LIST", "SEC_TRA_LIST", "ADDRESS_STREET_NUM_LIST", "ADDRESS_UNIT_NUM_LIST",
                    "ADDRESS_CITY_LIST", "ADDRESS_ZIP_EXTENSION_LIST", "ADDRESS_ZIP_LIST", "TAXES_LAND_VALUE_LIST",
                    "TAXES_IMPROVEMENT_VALUE_LIST", "TAXES_IMPROVEMENT_VALUE_LIST", "CLCA_LAND_VALUE_LIST",
                    "CLCA_IMPROVEMENT_VALUE_LIST", "FIXTURES_VALUE_LIST", "ADDRESS_STREET_NAME_LIST",
                    "PERSONAL_PROPERTY_VfALUE_LIST", "HPP_VALUE_LIST", "HOMEOWNERS_EXEMPTION_VALUE_LIST",
                    "OTHER_EXEMPTION_VALUE_LIST", "NET_TOTAL_VALUE_LIST", "LAST_DOC_PREFIX_LIST", "LAST_DOC_SERIES_LIST",
                    "LAST_DOC_DATE_LIST", "LAST_DOC_INPUT_DATE_LIST", "OWNER_NAME_LIST", "MA_CARE_OF_LIST",
                    "MA_ATTN_NAME_LIST", "MA_STREET_ADDRESS_LIST", "MA_UNIT_NUMBER_LIST", "MA_CITY_LIST",
                    "MA_STATE_LIST", "MA_ZIP_CODE_LIST", "MA_ZIP_CODE_EXTENSION_LIST", "MA_BARECODE_WALK_SEQ_LIST",
                    "MA_BARCODE_CHECK_DIGIT_LIST", "MA_EFFECTIVE_DATE_LIST", "MA_SOURCE_CODE_LIST", "USE_CODE_LIST",
                    "ECON_UNIT_FLAG_LIST", "APN_INACTIVE_DATE_LIST", "OWNER_NAME_CHANGE", "MA_STREET_ADDRESS_CHANGE",
                    "MA_CITY_CHANGE", "MA_STATE_CHANGE")

        df_groupby_parcel = df_groupby_parcel \
            .withColumn("OWNER_NAME_CHANGE", is_owner_change_udf(df["OWNER_NAME_LIST"])) \
            .withColumn("MA_STREET_ADDRESS_CHANGE", is_street_change_udf(df["MA_STREET_ADDRESS_LIST"])) \
            .withColumn("MA_CITY_CHANGE", is_city_change_udf(df["MA_CITY_LIST"])) \
            .withColumn("MA_STATE_CHANGE", is_state_change_udf(df["MA_STATE_LIST"])) \
            .withColumn("SOURCE_INFO_DATE", file_map[file]())

        df_groupby_parcel.write.format("org.apache.phoenix.spark") \
                .mode("overwrite") \
                .option("table", "ROLL_INFO_AGG") \
                .option("zkUrl", "namenode:2181") \
                .save()



main()