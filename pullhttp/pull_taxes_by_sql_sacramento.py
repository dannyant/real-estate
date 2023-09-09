import time
import traceback
from datetime import datetime

import phoenixdb.cursor
from base_http_pull import pull_http

database_url = 'http://namenode:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)


#curl 'https://eproptax.saccounty.net/servicev2/eproptax.svc/rest/BillSummary?parcel=25400310390000' \
#  -H 'Accept: */*' \
#  -H 'Accept-Language: en-US,en;q=0.9' \
#  -H 'Connection: keep-alive' \
#  -H 'Origin: https://eproptax.saccounty.gov' \
#  -H 'Referer: https://eproptax.saccounty.gov/' \
#  -H 'Sec-Fetch-Dest: empty' \
#  -H 'Sec-Fetch-Mode: cors' \
#  -H 'Sec-Fetch-Site: cross-site' \
#  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36' \
#  -H 'sec-ch-ua: "Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"' \
#  -H 'sec-ch-ua-mobile: ?0' \
#  -H 'sec-ch-ua-platform: "macOS"' \
#  --compressed

URL_ALAMEDA = 'https://eproptax.saccounty.net/servicev2/eproptax.svc/rest/BillSummary?parcel='
def pull_sacramento_taxes(parcel_id):
  property_tax_html = pull_http(URL_ALAMEDA + parcel_id, as_text=True)
  return property_tax_html

def pull_taxes(data):
  return pull_sacramento_taxes(parcel_id)


cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
isempty = False
while not isempty:
    try:
        cursor.execute("SELECT * FROM tax_info WHERE LAST_DOWNLOADED is null and COUNTY = 'SACRAMENTO' limit 100")
        all_parcel_dict = cursor.fetchall()
        isempty = len(all_parcel_dict) == 0
        print("count = " + str(len(all_parcel_dict)) + " \t " + str(datetime.now()))
        for parcel_dict in all_parcel_dict:
            county = parcel_dict["COUNTY"]
            parcel_id = parcel_dict["PARCEL_ID"]
            content = pull_taxes(parcel_id)
            if "System is temporarily unavailable" in content:
                raise Exception("Unavaliable")

            cursor.execute("UPSERT INTO tax_info (PARCEL_ID, COUNTY, HTML_CONTENTS, LAST_DOWNLOADED) VALUES (?, ?, ?, ?)", (parcel_id, county, content, str(datetime.now())))
            time.sleep(30)
    except Exception as ex:
        print(ex)
        traceback.print_exc()
        cursor.close()
        cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)

