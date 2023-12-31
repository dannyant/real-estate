import json
import traceback

from pyspark.sql import SparkSession
import re

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, StringType, BooleanType

current_tax_bill_re = re.compile("DISPLAY CURRENT BILL RESULTS[^$]*\\$([0-9,.]*).*DISPLAY PRIOR YEAR DELINQUENT TAX INFORMATION")
delinquent_tax_bill_re = re.compile("DISPLAY PRIOR YEAR DELINQUENT TAX INFORMATION[^$]*\\$([0-9,.]*).*DISPLAY TAX HISTORY")

def base_parse_tax_bill(regex, html_content):
    try:
        match = regex.search(html_content.replace("\n",""))
        if match is None:
            return None
        else:
            return_val = float(match.groups()[0].replace(",", ""))
            if return_val == 0:
                return None
            else:
                return return_val
    except:
        return None


def parse_current_tax_bill_alameda(html_content):
    return base_parse_tax_bill(current_tax_bill_re, html_content)

def parse_current_tax_bill_sacramento(json_content):
    try:
        if json_content is None:
            return None
        json_data = json.loads(json_content)
        return float(json_data["Bills"][0]["BillAmount"])
    except:
        traceback.print_exc()
        return None

def parse_delinquent_tax_bill_alameda(html_content):
    return base_parse_tax_bill(delinquent_tax_bill_re, html_content)

def parse_delinquent_tax_bill_sacramento(json_content):
    try:
        json_data = json.loads(json_content)
        if "Delinquent" not in json_data:
            return None
        unpaid = json_data["Delinquent"]
        return float(unpaid["RedemptionOutstanding"])
    except:
        traceback.print_exc()
        return None

current_function = {"ALAMEDA" : parse_current_tax_bill_alameda, "SACRAMENTO" : parse_current_tax_bill_sacramento}
delinquent_function = {"ALAMEDA" : parse_delinquent_tax_bill_alameda, "SACRAMENTO" : parse_delinquent_tax_bill_sacramento}

def current_taxes(county, html_content):
    return current_function[county](html_content)

def delinquent_taxes(county, html_content):
    return delinquent_function[county](html_content)

def interesting_property(delinquent_taxes):
    return delinquent_taxes is not None and delinquent_taxes > 10000

def max_date(county):
    if county == "ALAMEDA":
        return "2023-09-01"
    elif county == "SACRAMENTO":
        return "2023"
    else:
        return "UNKNOWN"


current_udf = udf(current_taxes, FloatType())
delinquent_udf = udf(delinquent_taxes, FloatType())
interesting_property_udf = udf(interesting_property, BooleanType())
max_date_udf = udf(max_date, StringType())


def main():
    # Initialize a Spark session
    spark = SparkSession.builder.appName("TaxProcessing").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", "tax_info") \
        .option("zkUrl", "namenode:2181").load()

    df = df.filter("LAST_DOWNLOADED is not NULL")\
        .withColumn("CURRENT_TAX_BILL", current_udf(df["COUNTY"], df["html_contents"])) \
        .withColumn("DELINQUENT_TAX_BILL", delinquent_udf(df["COUNTY"], df["html_contents"]))
    df = df.select("PARCEL_ID", "COUNTY", "CURRENT_TAX_BILL", "DELINQUENT_TAX_BILL")
    df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "TAX_INFO_STATUS") \
        .option("zkUrl", "namenode:2181") \
        .save()


    df = spark.read.format("org.apache.phoenix.spark").option("table", "TAX_INFO_STATUS") \
        .option("zkUrl", "namenode:2181").load()

    df = df.withColumn("SOURCE_INFO_DATE", max_date_udf(df["COUNTY"])) \
           .withColumn("INTERESTING_PROPERTY", interesting_property_udf(df["DELINQUENT_TAX_BILL"]))

    df.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "ROLL_INFO_AGG") \
        .option("zkUrl", "namenode:2181") \
        .save()


if __name__ == "__main__":
    main()
else:
    print("skipping")
