import pandas as pd
import json
import requests
import os
from base64 import b64decode
import datetime
from datetime import date
import uuid
import os


def runner():
    today = (date.today())
    api_key = '{}'

    base_url = "https://newsapi.org/v2/everything?q={}&from={}&to={}&sortBy=popularity&apiKey={}&language=en"
    print(base_url)
    start_date_value = str(today - datetime.timedelta(days=1))
    end_date_value = str(today)

    df = pd.DataFrame(columns=['newsTitle', 'timestamp', 'url_source', 'content', 'source', 'author', 'urlToImage'])

    url_extractor = base_url.format('Covid', start_date_value, end_date_value, api_key)
    print(url_extractor)
    response = requests.get(url_extractor)
    d = response.json()

    for i in d['articles']:
        newsTitle = i['title']
        timestamp = i['publishedAt']
        trimmed_part = "None"
        url_source = i['url']
        source = i['source']
        author = i['author']
        urlToImage = i['urlToImage']
        partial_content = ""
        if (str(i['content']) != 'None'):
            partial_content = i['content']
        if (len(partial_content) >= 200):
            partial_content = partial_content[0:199]
        if ('.' in partial_content):
            trimmed_part = partial_content[:partial_content.rindex('.')]
        else:
            trimmed_part = partial_content
        df = pd.concat([df, pd.DataFrame(
        {'newsTitle': newsTitle, 'timestamp': timestamp, 'url_source': url_source, 'content': trimmed_part,
        'source': source, 'author': author, 'urlToImage': urlToImage})], ignore_index=True)

    filename = str(uuid.uuid4())
    output_file = "/home/ubuntu/{}.parquet".format(filename)
    df1 = df.drop_duplicates()
    df1.to_parquet(output_file)
    return output_file