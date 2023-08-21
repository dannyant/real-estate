#!/usr/bin/python3.6
import gzip
import re

import xmltodict as xmltodict
from pyspark.shell import sc, spark
from pyspark.sql.types import StructType, StructField, StringType

from base_http_pull import pull_http


#https://stackoverflow.com/questions/16476413/how-to-insert-pandas-dataframe-via-mysqldb-into-database

schema = StructType([
    StructField("url", StringType()),
    StructField("site_last_mod", StringType())
])




def pull_sitemap_xml(sitemap, url_list):
    robots = pull_http(sitemap.strip(), as_text=False)
    print(sitemap.strip())
    robots_unzipped = gzip.decompress(robots).decode('utf-8')
    raw_robots = xmltodict.parse(robots_unzipped)
    properties_zip_url = raw_robots["sitemapindex"]["sitemap"]
    if isinstance(properties_zip_url, dict):
        properties_zip_url = [properties_zip_url]

    for prop_url_dict in properties_zip_url:
        loc_url = prop_url_dict["loc"]
        properties_zipped = pull_http(loc_url, as_text=False)
        properties_unzipped = gzip.decompress(properties_zipped).decode('utf-8')
        properties_xml = xmltodict.parse(properties_unzipped)
        for urlset in properties_xml:
            url_val = properties_xml[urlset]
            urls = url_val["url"]
            for url_dict in urls:
                new_dict = {}
                new_dict["url"] = url_dict["loc"]
                new_dict["last_mod"] = url_dict["lastmod"]
                url_list.append(new_dict)


def main():
    url = "https://www.apartments.com/robots.txt"
    robots = pull_http(url)
    p = re.compile('Sitemap: (.*)')
    robots_url = p.findall(robots)
    i = 0
    for robot in robots_url:
        robot = robot.strip()
        if ".gz" in robot and "AllNearMe" not in robot and "Profiles.xml.gz" not in robot and "Canada" not in robot and "ProvinceSearches.xml.gz" not in robot:
            try:
                urls = []
                pull_sitemap_xml(robot, urls)
                df = spark.createDataFrame(data=urls, schema=schema)
                df.write.format("org.apache.phoenix.spark") \
                    .mode("overwrite") \
                    .option("table", "apartments_property") \
                    .option("zkUrl", "192.168.1.162:2181") \
                    .save()
            except:
                pass
        i += 1


main()
