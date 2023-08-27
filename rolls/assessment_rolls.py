import os
from pathlib import Path

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType
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
    "MA street address": "MA_STREE_ADDRESS",
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
    StructField("MA_STREE_ADDRESS", StringType()),
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




def read_dataframe(filename, columns):
    df = pandas.read_csv(filename, encoding="ISO-8859-1", usecols=columns)
    print(df)

def parse_doc(doc_file):
    print(doc_file)
    text = docx2txt.process(doc_file)
    print(text)
    pass

def read_columns(dir):
    dir_list = os.listdir(dir)
    for d in dir_list:
        if "columns" in d.lower():
            columns_file = dir + "/" + d
            df = pandas.read_csv(columns_file, encoding="ISO-8859-1")
            return df.applymap(lambda x: column_translations[x] if x in column_translations else x)
    return None

def get_use_code_type(use_code):
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


def process_dir(dir):
    source_month = "SOURCE_MONTH"
    columns_df = read_columns(dir)
    columns = []
    for c in columns_df.values:
        columns.append(c[0])
    columns.append(source_month)
    dir_list = os.listdir(dir)
    rolls_file = None
    assessment_month = None
    for d in dir_list:
        filename = dir + "/" + d
        if "IE" in d and d.upper().endswith(".TXT"):
            rolls_file = filename

            p = Path(d)
            assessment_month = p.name.lower().replace(".txt", "").replace("ie670-","")

    if rolls_file is not None and columns is not None:
        df = pandas.read_csv(rolls_file, encoding="ISO-8859-1", header=None, sep="\t")
        df.columns = columns
        df[source_month] = assessment_month
        df['USE_CODE_DESC'] = df.apply(lambda x: get_use_code_type(x['USE_CODE']), axis=1)
        return df


#AUG_23_DATA = "/Users/dantonetti/soloprojects/real-estate/rolls/aug2023"
#aug_df = process_dir(AUG_23_DATA)
#aug_df.to_json("/Users/dantonetti/soloprojects/real-estate/rolls_aug2023.json")
#aug_df.add_prefix("aug23")
#MAY_22_DATA = "/Users/dantonetti/soloprojects/real-estate/rolls/may2022"
#may_df = process_dir(MAY_22_DATA)
#may_df.to_csv("/Users/dantonetti/soloprojects/real-estate/rolls_may2022.csv")
#may_df.add_prefix("may22")
#NOV_17_DATA = "/Users/dantonetti/soloprojects/real-estate/rolls/nov2017"
#nov_df = process_dir(NOV_17_DATA)
#nov_df.to_csv("/Users/dantonetti/soloprojects/real-estate/rolls_nov2017.csv")



def alameda():
    return "ALAMEDA"

def main():
    spark = SparkSession.builder.appName("ProcessRolls").getOrCreate()
    df = spark.read.csv("hdfs://namenode:8020/user/spark/apartments/rolls", sep="\t", schema=schema)
    df = df.select("APN_SHORT").withColumnRenamed("APN_SHORT","PARCEL_ID")
    alameda_udf = udf(alameda, StringType())
    df = df.withColumn("COUNTY", alameda_udf())


    df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "PARCEL_INFO") \
        .option("zkUrl", "namenode:2181") \
        .save()

