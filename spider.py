import json
from json import JSONDecodeError
from tqdm import tqdm
import logging
from bs4 import BeautifulSoup
from datetime import datetime

import requests
import asyncio
import aiohttp
from aiohttp import ContentTypeError

import pymongo
from pymongo import MongoClient
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# PyMongo
mongo_client = MongoClient()
mongo_db = mongo_client.weather

# Motor
client = AsyncIOMotorClient()
db = client.weather


class WeatherSpider:
    def __init__(self):
        self.main_url = "http://www.weather.com.cn/forecast/world.shtml"
        self.detail_url = "http://www.weather.com.cn/weather/{}.shtml"
        self.sess = None
        self.concurrency_num = 10
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
            'Referer': 'http://www.weather.com.cn/',
        }
        self.continent_names = [
            ['亚洲', 'Asia'],
            ['欧洲', 'Europe'],
            ['北美洲', 'North America'],
            ['南美洲', 'South America'],
            ['非洲', 'Africa'],
            ['大洋洲', 'Australia'],
        ]
        self.semaphore = asyncio.Semaphore(self.concurrency_num)

    def update_country(self):
        resp = requests.get(self.main_url, headers=self.headers)
        resp.encoding = 'utf-8'
        bs = BeautifulSoup(resp.text, 'html.parser')
        continents = bs.find_all('div', class_="guojia")
        # 设置 country 表的唯一索引
        mongo_db.country.create_index([('country_id', pymongo.ASCENDING)], unique=True)
        print('The Primary Key of Country:', sorted(list(mongo_db.country.index_information())))

        for i, c in enumerate(continents):
            p_list = c.find_all('p')
            for p in p_list:
                country = {
                    'country_id': int(p['data-id']),
                    'country_name_zh': p.find_all('span')[0].text,
                    'country_name_en': p.find_all('span')[1].text,
                    'continent_id': i,
                    'continent_name_zh': self.continent_names[i][0],
                    'continent_name_en': self.continent_names[i][1],
                    'last_crawl_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                mongo_db.country.update_one({'country_id': country['country_id']}, {'$set': country}, upsert=True)

    def update_city(self):
        mongo_db.city.create_index([('city_id', pymongo.ASCENDING)], unique=True)
        print('The Primary Key of City:', sorted(list(mongo_db.city.index_information())))
        countries = mongo_db.country.find({})
        with requests.session() as sess:
            sess.headers = self.headers
            # 这里必须显式地告诉 tqdm 这个迭代器的长度，否则无法显示进度条
            for country in tqdm(countries, total=mongo_db.country.count_documents({})):
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
                            mongo_db.city.update_one({'city_id': city['city_id']}, {'$set': city}, upsert=True)
                if 'sd' in data:  # 最后更新首都的 is_capital 字段，否则可能被覆盖
                    val = data['sd']
                    city.update({
                        'city_id': int(val['id']),
                        'city_name_zh': val['name'],
                        'city_name_en': val['en'],
                        'is_capital': True,
                        'last_crawl_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    mongo_db.city.update_one({'city_id': city['city_id']}, {'$set': city}, upsert=True)

    @staticmethod
    def parse_page(text, city_id):
        bs = BeautifulSoup(text, 'html.parser')
        result = {'city_id': city_id}
        for i, content in enumerate(bs.find('ul', class_="t clearfix").find_all('li')):
            date = content.find('h1').text
            weather = content.find('p', class_='wea').text
            temperature = content.find('p', class_='tem').text
            result[f'weather_{i + 1}'] = {
                'date': date,
                'weather': weather,
                'temperature': temperature.strip()
            }
        result['last_crawl_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return result

    async def scrape_api(self, url):
        async with self.semaphore:
            try:
                logging.info('scraping %s', url)
                async with self.sess.get(url) as resp:
                    await asyncio.sleep(1)
                    return await resp.text()
            except ContentTypeError:
                logging.error('error occurred while scraping %s', url, exc_info=True)

    @staticmethod
    async def save_data(data):
        logging.info('saving data %s', data['city_id'])
        if data:
            return await db.city.update_one({'city_id': data['city_id']}, {'$set': data}, upsert=True)

    async def scrape_weather(self, city_id):
        url = self.detail_url.format(city_id)
        text = await self.scrape_api(url)
        data = self.parse_page(text, city_id)
        await self.save_data(data)

    async def main(self):
        self.sess = aiohttp.ClientSession()
        scrape_detail_tasks = [
            asyncio.ensure_future(self.scrape_weather(city['city_id'])) for city in mongo_client.weather.city.find({})
        ]
        await asyncio.wait(scrape_detail_tasks)
        await self.sess.close()


if __name__ == '__main__':
    spider = WeatherSpider()
    spider.update_country()
    spider.update_city()
    asyncio.get_event_loop().run_until_complete(spider.main())