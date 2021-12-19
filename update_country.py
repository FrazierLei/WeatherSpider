import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.weather
url = "http://www.weather.com.cn/forecast/world.shtml"
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
    'Referer': 'http://www.weather.com.cn/',
}
resp = requests.get(url, headers=headers)
resp.encoding = 'utf-8'
bs = BeautifulSoup(resp.text, 'html.parser')
continents = bs.find_all('div', class_="guojia")

continent_names = [
    ['亚洲', 'Asia'],
    ['欧洲', 'Europe'],
    ['北美洲', 'North America'],
    ['南美洲', 'South America'],
    ['非洲', 'Africa'],
    ['大洋洲', 'Australia'],
]
# 设置 country 表的唯一索引
db.country.create_index([('country_id', pymongo.ASCENDING)], unique=True)
print('The Primary Key of Country:', sorted(list(db.country.index_information())))

for i, c in enumerate(continents):
    p_list = c.find_all('p')
    for p in p_list:
        country = {
            'country_id': int(p['data-id']),
            'country_name_zh': p.find_all('span')[0].text,
            'country_name_en': p.find_all('span')[1].text,
            'continent_id': i,
            'continent_name_zh': continent_names[i][0],
            'continent_name_en': continent_names[i][1],
            'last_crawl_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        db.country.update_one({'country_id': country['country_id']}, {'$set': country}, upsert=True)