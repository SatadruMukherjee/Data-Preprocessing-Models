import pandas as pd
import json
import requests
import os
from base64 import b64decode
import datetime
from datetime import date
today = (date.today())
five_days_past_today=str(today - datetime.timedelta(days=5))
four_days_past_today=str(today - datetime.timedelta(days=4))
three_days_past_today=str(today - datetime.timedelta(days=3))
two_days_past_today=str(today - datetime.timedelta(days=2))
one_days_past_today=str(today - datetime.timedelta(days=1))
api_key=''

base_url="https://newsapi.org/v2/everything?q={}&from={}&to={}&sortBy=popularity&apiKey={}&language=en"
print(base_url)
range_for_extraction=[five_days_past_today,four_days_past_today,three_days_past_today,two_days_past_today,
                      one_days_past_today,str(today)]


df=pd.DataFrame(columns=['newsTitle','timestamp','url_source','content','source','author','urlToImage'])

for i in range(0,len(range_for_extraction)-1):
    start_date_value=range_for_extraction[i]
    print(start_date_value)
    end_date_value=range_for_extraction[i+1]
    print(end_date_value)
    url_extractor=base_url.format('G20 Summit: India',start_date_value,end_date_value,api_key)
    response = requests.get(url_extractor)
    d = response.json()
    if (d['status']) == 'ok':
        for i in d['articles']:
            newsTitle = i['title']
            timestamp = i['publishedAt']
            trimmed_part = "None"

            url_source = i['url']

            source =i['source']

            author=i['author']

            urlToImage=i['urlToImage']

            partial_content = ""

            if (str(i['content']) != 'None'):
                partial_content = i['content']

            if (len(partial_content) >= 200):
                partial_content = partial_content[0:199]

            if ('.' in partial_content):
                trimmed_part = partial_content[:partial_content.rindex('.')]
            else:
                trimmed_part = partial_content
            df = df.append({'newsTitle': newsTitle,'timestamp':timestamp, 'url_source': url_source,'content':trimmed_part,'source':source,'author':author,'urlToImage':urlToImage}, ignore_index=True)


df.to_csv('C:/Users/USER/Downloads/data_extracted.csv',index=False)

