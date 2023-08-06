import time

import requests


def pull_http(url, as_text=True):

    # A GET request to the API
    response = requests.get(url, headers={"user-agent": "Mozilla/5.0"})

    #time.sleep(5)
    # Print the response
    if as_text:
        return response.text
    else:
        return response.content