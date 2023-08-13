#!/usr/bin/python3.6
import gzip
import re

import pandas
import pymysql as pymysql
import xmltodict as xmltodict
from pyspark.shell import sql

#from pyspark.shell import sql


from base_http_pull import pull_http


def pull_sitemap_xml(sitemap, url_list):
    print("Sitemap Processing " + str(sitemap))
    robots = pull_http(sitemap.strip(), as_text=False)
    robots_unzipped = gzip.decompress(robots).decode('utf-8')
    raw_robots = xmltodict.parse(robots_unzipped)
    properties_zip_url = raw_robots["sitemapindex"]["sitemap"]
    if type(properties_zip_url) is dict:
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
    print("Sitemap Startup")
    url = "https://www.apartments.com/robots.txt"
    robots = pull_http(url)
    p = re.compile('Sitemap: (.*)')
    robots_url = p.findall(robots)
    urls = []
    pull_sitemap_xml(robots_url[0], urls)
    #for robot in robots_url:
    #    if ".gz" not in robots:
    #        continue
    #    pull_sitemap_xml(robot, urls)

    #myrdd = sc.parallelize([urls])
    #df = spark.createDataFrame(data=myrdd, schema = schema)
    df = pandas.DataFrame(urls)

    conn = pymysql.connect(host="dannymain",
                           port=3306,
                           user="realestate",
                           passwd="password",
                           db="realestate",
                           charset='utf8')

    sql.write_frame(df, con=conn, name='apartments_property',
                    if_exists='replace', flavor='mysql')

    df.write.format("kafka")\
        .option("kafka.bootstrap.servers", "dannymain:9092")\
        .option("topic", "apartments_com_properties")\
        .save()

main()