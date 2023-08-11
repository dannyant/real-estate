#!/usr/bin/python
import requests


def pull_http(url, as_text=True, data=None):

    # A GET request to the API
    response = requests.get(url, headers={"user-agent": "Mozilla/5.0"}, data=data)

    #time.sleep(5)
    # Print the response
    if as_text:
        return response.text
    else:
        return response.content