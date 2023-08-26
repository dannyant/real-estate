from datetime import date

import phoenixdb.cursor
from base_http_pull import pull_http

database_url = 'http://192.168.1.162:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)

cursor = conn.cursor()
cursor.execute("SELECT * FROM apartments_property")
print(cursor.fetchall())

cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
cursor.execute("SELECT * FROM apartments_property WHERE last_downloaded is null")

all_url_dicts = cursor.fetchall()
print("count = " + str(len(all_url_dicts)))
for url_dict in all_url_dicts:
    url = url_dict["URL"]
    content = pull_http(url)
    cursor.execut