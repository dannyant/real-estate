import time
from datetime import date, datetime

import phoenixdb.cursor
from base_http_pull import pull_http

database_url = 'http://namenode:8765/'
conn = phoenixdb.connect(database_url, autocommit=True)

isempty = False
while not isempty:
    cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
    cursor.execute("SELECT * FROM apartments_property WHERE last_downloaded is null limit 100")
    all_url_dicts = cursor.fetchall()
    isempty = len(all_url_dicts) == 0
    print("count = " + str(len(all_url_dicts)))
    for url_dict in all_url_dicts:
        url = url_dict["URL"]
        content = pull_http(url)
        cursor.execute("UPSERT INTO apartments_property (URL, HTML_CONTENTS, LAST_DOWNLOADED) VALUES (?, ?, ?)", (url, content, str(datetime.now())))
        time.sleep(10)

