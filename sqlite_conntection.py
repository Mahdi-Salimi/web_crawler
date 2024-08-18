import aiohttp
import asyncio
import time
import logging
import aiosqlite


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def insert_data(results):
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
                    logger.info(
                        f"Inserted {insert_count} records into the database in {end_time - start_time:.2f} seconds.")

            except aiosqlite.Error as e:
                logger.error(f"An error occurred with the database: {e}")