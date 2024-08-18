import aiohttp
import asyncio
import aiosqlite
import time
import logging

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

sem = asyncio.Semaphore(5) # in main
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

async def fetch_data(session, url):
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

async def fetch_all_data(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(fetch_data(session, url)) for url in urls]
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
    urls = [f'https://bama.ir/cad/api/search?pageIndex={i}' for i in range(10)]

    async with aiosqlite.connect('cars.db') as db:
        async with db.cursor() as cursor:
            try:
                await cursor.execute('''
                    CREATE TABLE IF NOT EXISTS cars_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT,
                        title TEXT,
                        time TEXT,
                        year TEXT,
                        image TEXT,
                        mileage TEXT,
                        location TEXT,
                        description TEXT,
                        created_at TEXT,
                        price TEXT
                    );
                ''')
                await db.commit()
                logger.info("Table created successfully")

                results = await fetch_all_data(urls)

                async with db.cursor() as cursor:
                    insert_count = 0
                    start_time = time.time()
                    for url, cars in results:
                        if not cars:
                            logger.error(f"Failed to fetch data from {url}")
                            continue

                        for car in cars:
                            await cursor.execute('''
                                INSERT INTO cars_new (url, title, time, year, mileage, location, description, image, created_at, price)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                            ''', car)
                            insert_count += 1

                    await db.commit()
                    end_time = time.time()
                    logger.info(f"Inserted {insert_count} records into the database in {end_time - start_time:.2f} seconds.")

            except aiosqlite.Error as e:
                logger.error(f"An error occurred with the database: {e}")

asyncio.run(main())

# semaphore in main , because unwanted event loops