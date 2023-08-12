#!/usr/bin/python3.6
import gzip
import re

import pandas
import xmltodict as xmltodict
from pyspark.shell import spark, sc
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StructType, StructField, StringType

from base_http_pull import pull_http


@udf
def pull_sitemap_xml(sitemap):
    print("Sitemap Processing " + str(sitemap))
    robots = pull_http(sitemap.strip(), as_text=False)
    robots_unzipped = gzip.decompress(robots).decode('utf-8')
    raw_robots = xmltodict.parse(robots_unzipped)
    properties_zip_url = raw_robots["sitemapindex"]["sitemap"]["loc"]
    properties_zipped = pull_http(properties_zip_url, as_text=False)
    properties_unzipped = gzip.decompress(properties_zipped).decode('utf-8')
    properties_xml = xmltodict.parse(properties_unzipped)
    properties_data = properties_xml["urlset"]["url"]
    print(properties_data)
    print("Sitemap Done Processing " + str(properties_data))
    return pandas.DataFrame(properties_data)

def main():
    print("Sitemap Startup")
    url = "https://www.apartments.com/robots.txt"
    robots = pull_http(url)
    p = re.compile('Sitemap: (.*)')
    robots_url = p.findall(robots)
    schema = StructType([
        StructField("url", StringType())
    ])

    myrdd = sc.parallelize([robots_url])

    print("Sitemap RDD + " + str(myrdd.collect()))
    df = spark.createDataFrame(data=myrdd, schema = schema)
    df_url = pull_sitemap_xml(df["url"])
    print("Sitemap RDD + " + str(df_url))
    df.write.format("kafka")\
        .option("kafka.bootstrap.servers", "dannymain:9092")\
        .option("topic", "apartments_com_properties")\
        .save()

main()