import time
from datetime import datetime

import phoenixdb.cursor
from base_http_pull import pull_http

database_url = 'http://namenode:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)

URL_ALAMEDA = 'https://www.acgov.org/ptax_pub_app/RealSearch.do'
def pull_alameda_taxes(property_data):
  data = {
    'displayApn': property_data["PARCEL_ID"],
    'searchBills': 'Search',
  }
  property_tax_html = pull_http(URL_ALAMEDA, as_text=True, data=data)
  return property_tax_html

region_function_mapping = {
  "ALAMEDA" : pull_alameda_taxes
}

def pull_taxes(county, data):
  return region_function_mapping[county](data)


cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
isempty = False
while not isempty:
    try:
        cursor.execute("SELECT * FROM tax_info WHERE LAST_DOWNLOADED is null limit 100")
        all_parcel_dict = cursor.fetchall()
        isempty = len(all_parcel_dict) == 0
        print("count = " + str(len(all_parcel_dict)) + " \t " + str(datetime.now()))
        for parcel_dict in all_parcel_dict:
            county = parcel_dict["COUNTY"]
            parcel_id = parcel_dict["PARCEL_ID"]
            content = pull_taxes(county, parcel_dict)
            cursor.execute("UPSERT INTO tax_info (PARCEL_ID, COUNTY, HTML_CONTENTS, LAST_DOWNLOADED) VALUES (?, ?, ?, ?)", (parcel_id, county, content, str(datetime.now())))
            time.sleep(30)
    except:
        cursor.close()
        cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)

