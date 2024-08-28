import aiohttp
import asyncio
import time
import logging
from sqlite_connection import insert_data


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_car_json_data(response_json):
    cars=[]
    cars_count = len(response_json['data']['ads'])
    for i in range(cars_count):
        car = response_json['data']['ads'][i]['detail']
        detail_keys = ['url', 'title', 'time', 'year',  'mileage', 'location', 'description', 'image',
                       'modified_date']

        price = response_json['data']['ads'][0]['price']['price']
        values = [car[key] for key in detail_keys]
        values.append(price)
        cars.append(values)
    return cars

async def fetch_data(sem, session, url): # pass sem
    async with sem:
        try:
            logger.info(f"Fetching data from {url}")
            start_time = time.time()
            async with session.get(url, headers=None, ssl=False) as response:
                logger.info(f"Response status for {url}: {response.status}")

                if response.status == 200:
                    response_json = await response.json()
                    cars = extract_car_json_data(response_json)
                    end_time = time.time()
                    logger.info(f"Fetched {len(cars)} cars from {url} in {end_time - start_time:.2f} seconds")
                    return url, cars
                else:
                    logger.error(f"Unexpected response status {response.status} for {url}")
                    return url, []
        except aiohttp.ClientError as e:
            logger.error(f"Client error fetching data from {url}: {e}")
            return url, []
        except asyncio.TimeoutError as e:
            logger.error(f"Timeout error fetching data from {url}: {e}") # set time out
            return url, []
        except Exception as e:
            logger.error(f"Unexpected error fetching data from {url}: {e}")
            return url, []

async def fetch_all_data(sem, urls):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(fetch_data(sem, session, url)) for url in urls]
        logger.info("Starting gather")
        start_time = time.time()
        try:
            results = await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Exception during gather: {e}")
            results = []
        end_time = time.time()
        logger.info(f"Completed gather in {end_time - start_time:.2f} seconds")
        return results

async def main():
    sem = asyncio.Semaphore(5)
    urls = [f'https://bama.ir/cad/api/search?pageIndex={i}' for i in range(10)]
    results = await fetch_all_data(sem, urls)
    await insert_data(results)


asyncio.run(main())

