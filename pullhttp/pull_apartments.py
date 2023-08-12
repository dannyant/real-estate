#!/usr/bin/python3.6
import gzip
import re

import xmltodict as xmltodict
from pyspark.shell import sc, spark

from base_http_pull import pull_http


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
    return properties_data

def main():
    print("Sitemap Startup")
    url = "https://www.apartments.com/robots.txt"
    robots = pull_http(url)
    p = re.compile('Sitemap: (.*)')
    robots_url = p.findall(robots)
    urls = []
    for robot in robots_url:
        urls += pull_sitemap_xml(robot)

    myrdd = sc.parallelize([urls])
    df = spark.createDataFrame(data=myrdd, schema = schema)

    df.write.format("kafka")\
        .option("kafka.bootstrap.servers", "dannymain:9092")\
        .option("topic", "apartments_com_properties")\
        .save()

main()