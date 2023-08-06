import gzip
import re

import pandas as pandas
import xmltodict as xmltodict

from pyspark.sql.functions import pandas_udf
from pyspark.sql import SparkSession


from base_http_pull import pull_http

def pull_sitemap_xml(sitemap):
    robots = pull_http(sitemap.strip(), as_text=False)
    robots_unzipped = gzip.decompress(robots).decode('utf-8')
    raw_robots = xmltodict.parse(robots_unzipped)
    properties_zip_url = raw_robots["sitemapindex"]["sitemap"]["loc"]
    properties_zipped = pull_http(properties_zip_url, as_text=False)
    properties_unzipped = gzip.decompress(properties_zipped).decode('utf-8')
    properties_xml = xmltodict.parse(properties_unzipped)
    properties_data = properties_xml["urlset"]["url"]
    return properties_data

def main(spark):
    url = "https://www.apartments.com/robots.txt"
    robots = pull_http(url)
    p = re.compile('Sitemap: (.*)')
    robots_url = p.findall(robots)

    df = spark.createDataFrame(robots_url, ("url"))
    property_urls = df.applymap(lambda x: pull_sitemap_xml(x))
    property_urls.writeStream().format("kafka").outputMode("append")\
        .option("kafka.bootstrap.servers", "192.168.1.100:9092")\
        .option("topic", "apartments_com_properties")\
        .start()\
        .awaitTermination()


if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate())