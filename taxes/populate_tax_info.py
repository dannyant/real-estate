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

def main():
    spark = SparkSession.builder.appName("PopulateTaxInfo").getOrCreate()

    df = spark.read.format("org.apache.phoenix.spark").option("table", "ROLL_INFO") \
        .option("zkUrl", "namenode:2181").load()

    df = df.select("PARCEL_ID", "COUNTY", "USE_TYPE")

    df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "tax_info") \
        .option("zkUrl", "namenode:2181") \
        .save()



main()