import requests
import json
from json import JSONDecodeError
from datetime import datetime
import pymongo
from pymongo import MongoClient
from tqdm import tqdm

client = MongoClient()
db = client.weather
# 设置 city 表的唯一索引
db.city.create_index([('city_id', pymongo.ASCENDING)], unique=True)
print('The Primary Key of City:', sorted(list(db.city.index_information())))
countries = db.country.find()

with requests.session() as sess:
    sess.headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
        'Referer': 'http://www.weather.com.cn/',
    }
    # 这里必须显式地告诉 tqdm 这个迭代器的长度，否则无法显示进度条
    for country in tqdm(countries, total=db.country.count_documents({})):
        country_id = int(country['country_id'])
        country_name_zh = country['country_name_zh']
        country_name_en = country['country_name_en']
        continent_id = country['continent_id']
        continent_name_zh = country['continent_name_zh']
        continent_name_en = country['continent_name_en']
        resp = sess.get(f'http://d1.weather.com.cn/gw/gj{country_id}.html')
        resp.encoding = 'utf-8'
        # 多米尼克这个国家的网页有点问题，少个引号，但是这个 bug 随时可能被修复
        try:
            data = json.loads(resp.text.split('=', 1)[1])
        except JSONDecodeError:
            data = json.loads(resp.text.replace('name', '"name', 1).split('=', 1)[1])
        city = {
            'country_id': country_id,
            'country_name_zh': country_name_zh,
            'country_name_en': country_name_en,
            'continent_id': continent_id,
            'continent_name_zh': continent_name_zh,
            'continent_name_en': continent_name_en,
        }
        for val in data.values():
            if isinstance(val, list):  # 普通城市
                for v in val:
                    city.update({
                        'city_id': int(v['id']),
                        'city_name_zh': v['name'],
                        'city_name_en': v['en'],
                        'is_capital': False,
                        'last_crawl_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    db.city.update_one({'city_id': city['city_id']}, {'$set': city}, upsert=True)
        if 'sd' in data:  # 最后更新首都的 is_capital 字段，否则可能被覆盖
            val = data['sd']
            city.update({
                'city_id': int(val['id']),
                'city_name_zh': val['name'],
                'city_name_en': val['en'],
                'is_capital': True,
                'last_crawl_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            db.city.update_one({'city_id': city['city_id']}, {'$set': city}, upsert=True)