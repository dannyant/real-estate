import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from base_http_pull import pull_http

URL_ALAMEDA = 'https://www.acgov.org/ptax_pub_app/RealSearch.do'
def pull_alameda_taxes(property_data):
  data = {
    'displayApn': property_data["apn"],
    'searchBills': 'Search',
  }
  property_tax_html = pull_http(URL_ALAMEDA, as_text=True, data=data)
  return property_tax_html

region_function_mapping = {
  "Alameda" : pull_alameda_taxes
}

def pull_taxes(county, data):
  return region_function_mapping[county](data)

download_udf = udf(pull_taxes, StringType())
datetimenow = udf(getdatetimenow, StringType())


def main():
    isempty = False
    while not isempty:
        # Initialize a Spark session
        spark = SparkSession.builder.appName("TaxDownload").getOrCreate()
        df = spark.read.format("org.apache.phoenix.spark").option("table", "tax_info")\
            .option("zkUrl", "namenode:2181").load()
        df = df.filter("last_downloaded is NULL")
        df = df.limit(100)
        isempty = df.count() == 0
        df_with_content = df.withColumn("html_contents", download_udf(df["url"]))
        df_with_content = df_with_content.withColumn("last_downloaded", datetimenow())
        df_with_content.write.format("org.apache.phoenix.spark") \
            .mode("overwrite") \
            .option("table", "apartments_property") \
            .option("zkUrl", "namenode:2181") \
            .save()
        time.sleep(100)

print("NAME == " + str(__name__))
if __name__ == "__main__":
    main()
else:
    print("skipping")
