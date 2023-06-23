import asyncio
import json
from os import environ
from time import time, sleep
from faker import Faker
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.orm import declarative_base

faker = Faker()

Base = declarative_base()
psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)

class Devices(Base):
    __tablename__ = 'devices'
    id = Column(Integer, primary_key=True, autoincrement=True)
    device_id = Column(String)
    temperature = Column(Integer)
    location = Column(String)
    time = Column(String)
	
while True:
    try:
        Base.metadata.create_all(psql_engine) 
        break
    except OperationalError:
        sleep(0.1)

Session = sessionmaker(bind=psql_engine)
session = Session()


async def store_data_point(device_id):
    with psql_engine.connect() as conn:
        while True:
            data = Devices(
                device_id=device_id,
                temperature=faker.random_int(10, 50),
                location=json.dumps(dict(latitude=str(faker.latitude()), longitude=str(faker.longitude()))),
                time=str(int(time()))
            )
            session.add(data)
            session.commit()
            print(device_id, data.time)
            await asyncio.sleep(1.0)


loop = asyncio.get_event_loop()
asyncio.ensure_future(
    store_data_point(
        device_id=str(faker.uuid4())
    )
)

asyncio.ensure_future(
    store_data_point(
        device_id=str(faker.uuid4())
    )
)

asyncio.ensure_future(
    store_data_point(
        device_id=str(faker.uuid4())
    )
)

loop.run_forever()
