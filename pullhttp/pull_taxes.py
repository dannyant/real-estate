from base_http_pull import pull_http

#curl 'https://www.acgov.org/ptax_pub_app/RealSearch.do' \
#  --data-raw 'displayApn=13-1160-43&searchBills=Search' \
#  --compressed


URL_ALAMEDA = 'https://www.acgov.org/ptax_pub_app/RealSearch.do'
def pull_alameda_taxes(property_data):
  data = {
    'displayApn': property_data["apn"],
    'searchBills': 'Search',
  }
  property_tax_html = pull_http(URL_ALAMEDA, as_text=True, data=data)
  return property_tax_html

region_function_mapping = {
  "Alameda" : pull_alameda_taxes
}

def pull_taxes(county, data):
  return region_function_mapping[county](data)

def main(spark):
  df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "dannymain:9092") \
    .option("subscribe", "apn_data") \
    .option("startingOffsets", "earliest") \
    .load()

  property_urls = df.applymap(lambda x: pull_taxes(x))
  property_urls.writeStream().format("kafka").outputMode("append") \
      .option("kafka.bootstrap.servers", "dannymain:9092") \
      .option("topic", "tax_data") \
      .start() \
      .awaitTermination()
