import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from base_http_pull import pull_http


def download_url_content(url):
    try:
        print("requesting url = " + str(url))
        response = pull_http(url)
        print("response = " + str(len(response)))
        return response.read().decode('utf-8')  # Assuming UTF-8 encoding
    except Exception as e:
        print(f"""{e}, quitting""")
        sys.exit(1)

def main():
    # Initialize a Spark session
    spark = SparkSession.builder.appName("AptUrlDownload").getOrCreate()
    df = spark.read.format("org.apache.phoenix.spark").option("table", "apartments_property")\
        .option("zkUrl", "namenode:2181").load()

    print("got data")
    # Register UDF to download content
    download_udf = udf(download_url_content, StringType())
    # Add a new column with downloaded content
    print("setup udf")
    df = df.limit(10)

    print("limit df")
    df_with_content = df.withColumn("html_content", download_udf(df["url"]))
    # Show the DataFrame with downloaded content
    df_with_content.show(truncate=False)
    print("shown")

    df_with_content.write.format("org.apache.phoenix.spark") \
        .mode("overwrite") \
        .option("table", "apartments_property") \
        .option("zkUrl", "192.168.1.162:2181") \
        .save()
    print("saved")

print("NAME == " + str(__name__))
if __name__ == "__main__":
    main()
else:
    print("skipping")
