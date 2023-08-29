import re
import time
import traceback
from datetime import datetime

import phoenixdb.cursor
from base_http_pull import pull_http
import urllib.parse

database_url = 'http://namenode:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)

BING_SEARCH = 'https://www.bing.com/search?q=SEARCHTERM+site%3Aapartments.com'
#10582+Topanga+Dr%2C+Oakland

apartment_url_re = re.compile("(https://www.apartments.com[^\"]*)\"")

cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
isempty = False
while not isempty:
    try:
        cursor.execute("SELECT * FROM ADDRESS_INFO WHERE LAST_DOWNLOADED is null limit 100")
        all_parcel_dict = cursor.fetchall()
        isempty = len(all_parcel_dict) == 0
        print("count = " + str(len(all_parcel_dict)) + " \t " + str(datetime.now()))
        for parcel_dict in all_parcel_dict:
            county = parcel_dict["COUNTY"]
            parcel_id = parcel_dict["PARCEL_ID"]
            use_type = parcel_dict["USE_TYPE"]
            if use_type == "COMMERCIAL_RESIDENTIAL":
                city = parcel_dict["CITY"]
                street_name = parcel_dict["STREET_NAME"]
                street_number = parcel_dict["STREET_NUMBER"]
                zipcode = parcel_dict["ZIPCODE"]
                if street_number is None:
                    url = None
                else:
                    query = street_number + " " + street_name + " " + city + " " + zipcode
                    query = query.replace(" ", "+")
                    url = BING_SEARCH.replace("SEARCHTERM", query)
                    html_content = pull_http(url)
                    matches = apartment_url_re.search(html_content)
                    if matches is None:
                        url = None
                    else:
                        groups = matches.groups()
                        if groups is None:
                            url = None
                        else:
                            url = groups[0]
            else:
                url = None
            if url is not None and "&" in url:
                url = None
            cursor.execute("UPSERT INTO ADDRESS_INFO (COUNTY, PARCEL_ID, URL, LAST_DOWNLOADED) VALUES (?, ?, ?, ?)", (county, parcel_id, url, str(datetime.now())))
            if url is not None:
                time.sleep(5)
    except Exception as ex:
        print(ex)
        traceback.print_exc()
        cursor.close()
        cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)

