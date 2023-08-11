#!/usr/bin/python
import gzip
import re

import xmltodict as xmltodict
from pyspark.shell import spark, sc
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StructType, StructField, StringType

from base_http_pull import pull_http

@udf
def pull_sitemap_xml(sitemap):
    robots = pull_http(sitemap.strip(), as_text=False)
    robots_unzipped = gzip.decompress(robots).decode('utf-8')
    raw_robots = xmltodict.parse(robots_unzipped)
    properties_zip_url = raw_robots["sitemapindex"]["sitemap"]["loc"]
    properties_zipped = pull_http(properties_zip_url, as_text=False)
    properties_unzipped = gzip.decompress(properties_zipped).decode('utf-8')
    properties_xml = xmltodict.parse(properties_unzipped)
    properties_data = properties_xml["urlset"]["url"]
    print(properties_data)
    return properties_data

def main():
    url = "https://www.apartments.com/robots.txt"
    robots = pull_http(url)
    p = re.compile('Sitemap: (.*)')
    robots_url = p.findall(robots)
    schema = StructType([
        StructField("url", StringType())
    ])
    myrdd = sc.parallelize(robots_url)

    df = spark.createDataFrame(data=myrdd, schema = schema)
    df = df.withColumn(
        "property_url",
        lit(pull_sitemap_xml(df["column1"]))
    )
    df.writeStream().format("kafka").outputMode("append")\
        .option("kafka.bootstrap.servers", "dannymain:9092")\
        .option("topic", "apartments_com_properties")\
        .start()\
        .awaitTermination()


main()