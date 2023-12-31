import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from base_http_pull import pull_http


def download_url_content(url):
    try:
        print("url=" + str(url))
        response = pull_http(url)
        time.sleep(10)
        return response
    except Exception as e:
        print("BAD URL " + str(url))
        return None

def getdatetimenow():
    return str(datetime.now())


download_udf = udf(download_url_content, StringType())
datetimenow = udf(getdatetimenow, StringType())


def main():
    isempty = False
    while not isempty:
        # Initialize a Spark session
        spark = SparkSession.builder.appName("AptUrlDownload").getOrCreate()
        df = spark.read.format("org.apache.phoenix.spark").option("table", "apartments_property")\
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
