import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from base_http_pull import pull_http


def download_url_content(url):
    try:
        print("url" + str(url))
        response = pull_http(url)
        time.sleep(30)
        return response
    except Exception as e:
        print("BAD URL " + str(url))
        return None

def getdatetimenow():
    return str(datetime.now())


def main():
    # Initialize a Spark session
    spark = SparkSession.builder.appName("AptUrlDownload").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", "apartments_property")\
        .option("zkUrl", "namenode:2181").load()
    df = df.limit(1000)

    # Register UDF to download content
    download_udf = udf(download_url_content, StringType())
    datetimenow = udf(getdatetimenow, StringType())
    # Add a new column with downloaded content

    df_with_content = df.withColumn("html_contents", download_udf(df["url"]))
    df_with_content = df_with_content.withColumn("last_downloaded", datetimenow())
    # Show the DataFrame with downloaded content

    df_with_content.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "apartments_property") \
        .option("maxRecordsPerFile", 100) \
        .option("zkUrl", "namenode:2181") \
        .save()
    print("saved")

print("NAME == " + str(__name__))
if __name__ == "__main__":
    main()
else:
    print("skipping")
