import logging
import time
import re
from datetime import datetime

def parse_mileage(mileage_str):
    try:
        numeric_part = re.sub(r'[^\d.]', '', mileage_str)
        return float(numeric_part)
    except (ValueError, TypeError):
        return None

from sqlalchemy import create_engine, Column, Integer, String, Float, Text, DateTime, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = "mysql+pymysql://root:digitoon_bama@localhost:3306/cars_db"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Car(Base):
    __tablename__ = 'cars_new'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    url = Column(String(255), nullable=False)
    title = Column(String(255), nullable=False)
    time = Column(DateTime, nullable=False)
    year = Column(Integer, nullable=False)
    mileage = Column(Float, nullable=True)
    location = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    image = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    price = Column(Float, nullable=True)


Base.metadata.create_all(bind=engine)

async def insert_data(results):
    session = SessionLocal()
    try:
        insert_count = 0
        start_time = time.time()

        for url, cars in results:
            if not cars:
                logger.error(f"Failed to fetch data from {url}")
                continue

            def parse_price(price_str):
                try:
                    numeric_part = price_str.replace(',', '')
                    return float(numeric_part)
                except (ValueError, TypeError):
                    return None

            for url, cars in results:
                if not cars:
                    logger.error(f"Failed to fetch data from {url}")
                    continue

                for car in cars:
                    try:
                        parsed_time = datetime.strptime(car[2], "%Y-%m-%d %H:%M:%S")
                    except (ValueError, TypeError):
                        parsed_time = datetime.utcnow()

                    try:
                        created_at_time = datetime.strptime(car[8], "%Y-%m-%dT%H:%M:%S.%f") if car[
                            8] else datetime.utcnow()
                    except (ValueError, TypeError):
                        created_at_time = datetime.utcnow()

                    price_value = parse_price(car[9]) if car[9] else None

                    new_car = Car(
                        url=car[0],
                        title=car[1],
                        time=parsed_time,
                        year=int(car[3]),
                        mileage=parse_mileage(car[4]) if car[4] else None,
                        location=car[5],
                        description=car[6],
                        image=car[7],
                        created_at=created_at_time,
                        price=price_value
                    )
                    session.add(new_car)
                    insert_count += 1

        session.commit()
        end_time = time.time()
        logger.info(f"Inserted {insert_count} records into the database in {end_time - start_time:.2f} seconds.")

    except SQLAlchemyError as e:
        logger.error(f"An error occurred with the database: {e}")
        session.rollback()
    finally:
        session.close()

