import asyncio
import json
import time

import aiohttp
import logging
from bs4 import BeautifulSoup
from aiohttp import ContentTypeError
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')

client = AsyncIOMotorClient()
db = client['weather']
collection = db['city']
mongo_client = MongoClient()
CONCURRENCY = 5
semaphore = asyncio.Semaphore(CONCURRENCY)

def parse_page(text, id):
    bs = BeautifulSoup(text, 'html.parser')
    result = {'city_id': id}
    for i, content in enumerate(bs.find('ul', class_="t clearfix").find_all('li')):
        date = content.find('h1').text
        weather = content.find('p', class_='wea').text
        temperature = content.find('p', class_='tem').text
        result[f'weather_{i + 1}'] = {
            'date': date,
            'weather': weather,
            'temperature': temperature
        }
    return result

async def scrape_api(url):
    async with semaphore:
        try:
            logging.info('scraping %s', url)
            async with sess.get(url) as resp:
                await asyncio.sleep(1)
                return await resp.text()
        except ContentTypeError as e:
            logging.error('error occurred while scraping %s', url, exc_info=True)

async def save_data(data):
    logging.info('saving data %s', data['city_id'])
    if data:
        return await collection.update_one({'city_id': data['city_id']}, {'$set': data}, upsert=True)

async def scrape_detail(id):
    url = f"http://www.weather.com.cn/weather/{id}.shtml"
    text = await scrape_api(url)
    data = parse_page(text, id)
    await save_data(data)

async def main():
    global sess
    sess = aiohttp.ClientSession()
    scrape_detail_tasks = [
        asyncio.ensure_future(scrape_detail(city['city_id'])) for city in mongo_client.weather.city.find({})
    ]
    await asyncio.wait(scrape_detail_tasks)
    await sess.close()

if __name__ == '__main__':
    # asyncio.run(main())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())