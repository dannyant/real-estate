import traceback

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, NumericType, IntegerType
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

def main():
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
            .withColumn("TAXES_LAND_VALUE", to_int_conv(df["TAXES_LAND_VALUE"])) \
            .withColumn("TAXES_IMPROVEMENT_VALUE", to_int_conv(df["TAXES_IMPROVEMENT_VALUE"])) \
            .withColumn("CLCA_LAND_VALUE", to_int_conv(df["CLCA_LAND_VALUE"])) \
            .withColumn("CLCA_IMPROVEMENT_VALUE", to_int_conv(df["CLCA_IMPROVEMENT_VALUE"])) \
            .withColumn("FIXTURES_VALUE", to_int_conv(df["FIXTURES_VALUE"])) \
            .withColumn("PERSONAL_PROPERTY_VALUE", to_int_conv(df["PERSONAL_PROPERTY_VALUE"])) \
            .withColumn("HPP_VALUE", to_int_conv(df["HPP_VALUE"])) \
            .withColumn("HOMEOWNERS_EXEMPTION_VALUE", to_int_conv(df["HOMEOWNERS_EXEMPTION_VALUE"])) \
            .withColumn("OTHER_EXEMPTION_VALUE", to_int_conv(df["OTHER_EXEMPTION_VALUE"])) \
            .withColumn("NET_TOTAL_VALUE", to_int_conv(df["NET_TOTAL_VALUE"]))


        df = df.select("COUNTY", "PARCEL_ID", "USE_TYPE", "SOURCE_INFO_DATE", "PRI_TRA", "SEC_TRA", "ADDRESS_STREET_NUM",
                       "ADDRESS_STREET_NAME", "ADDRESS_UNIT_NUM", "ADDRESS_CITY", "ADDRESS_ZIP", "ADDRESS_ZIP_EXTENSION",
                        "TAXES_LAND_VALUE", "TAXES_IMPROVEMENT_VALUE", "CLCA_LAND_VALUE", "CLCA_IMPROVEMENT_VALUE",
                       "FIXTURES_VALUE", "PERSONAL_PROPERTY_VALUE", "HPP_VALUE", "HOMEOWNERS_EXEMPTION_VALUE",
                       "OTHER_EXEMPTION_VALUE", "NET_TOTAL_VALUE", "LAST_DOC_PREFIX", "LAST_DOC_SERIES", "LAST_DOC_DATE",
                       "LAST_DOC_INPUT_DATE", "OWNER_NAME", "MA_CARE_OF", "MA_ATTN_NAME", "MA_STREET_ADDRESS",
                       "MA_UNIT_NUMBER", "MA_CITY_STATE", "MA_ZIP_CODE", "MA_ZIP_CODE_EXTENSION", "MA_BARECODE_WALK_SEQ",
                       "MA_BARCODE_CHECK_DIGIT", "MA_EFFECTIVE_DATE", "MA_SOURCE_CODE", "USE_CODE", "ECON_UNIT_FLAG",
                        "APN_INACTIVE_DATE")

        df.write.format("org.apache.phoenix.spark") \
                .mode("overwrite") \
                .option("table", "ROLL_INFO") \
                .option("zkUrl", "namenode:2181") \
                .save()

        df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
            .option("zkUrl", "namenode:2181").load()
        df = df.withColumn("USE_TYPE", use_code_type(df["USE_CODE"]))

        df_groupby_parcel = df.groupby("COUNTY", "PARCEL_ID")
        df_groupby_parcel.show(1)



main()