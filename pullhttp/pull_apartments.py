#!/usr/bin/python3.6
import gzip
import re

import xmltodict as xmltodict
from pyspark.shell import sc, spark

from base_http_pull import pull_http


def pull_sitemap_xml(sitemap, url_list):
    print("Sitemap Processing " + str(sitemap))
    robots = pull_http(sitemap.strip(), as_text=False)
    robots_unzipped = gzip.decompress(robots).decode('utf-8')
    print("RAW RETURN " + str(robots_unzipped))
    raw_robots = xmltodict.parse(robots_unzipped)
    properties_zip_url = raw_robots["sitemapindex"]["sitemap"]
    print("DIcT TYPE = " + str(dict))
    if isinstance(properties_zip_url, dict):
        print("DICT = " + str(properties_zip_url))
        properties_zip_url = [properties_zip_url]
    else:
        print(isinstance(properties_zip_url, dict))
        print("NOT DICT = " + str(type(properties_zip_url)))

    for prop_url_dict in properties_zip_url:
        print("ZIP URL = " + str(prop_url_dict))
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
    urls = []
    i = 0
    for robot in robots_url:
        if ".gz" in robots and i == 0:
            pull_sitemap_xml(robot, urls)
        i += 1

    print(urls)
    myrdd = sc.parallelize([urls])
    df = spark.createDataFrame(data=myrdd)

    df.write.format("kafka")\
        .option("kafka.bootstrap.servers", "dannymain:9092")\
        .option("topic", "apartments_com_properties")\
        .save()

main()