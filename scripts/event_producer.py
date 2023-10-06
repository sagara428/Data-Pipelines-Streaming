'''
This script simulates a data generator that produces synthetic data
and publishes it to a Kafka topic using a Kafka producer.

The script performs the following tasks:
1. Reads Kafka-related configuration from environment variables loaded from a .env file
2. Generates fake data for orders, including:
    order IDs, customer IDs, furniture types, colors, prices, and timestamps.
3. Encodes the generated data as JSON and sends it to a specified Kafka topic.

The `DataGenerator` class provides a method for generating fake purchasing events.
The data includes:
- `order_id`: A UUID string representing a unique order identifier.
- `customer_id`: A random integer between 1 and 100, representing a customer's ID.
- `furniture`: A random furniture type, such as Chair, Table, Desk, Sofa, or Bed.
- `color`: A random color name for the furniture.
- `price`: A random price for the furniture within the range of 100 to 150,000.
- `ts`: A random timestamp within the last hour.

The script runs indefinitely, continuously generating and publishing data
to the specified Kafka topic, with a 6-second interval between each data publication.

This script is typically used for testing and demonstration purposes,
where a stream of synthetic data is needed for Kafka-related applications.
'''
import json
import uuid
import os
from pathlib import Path
from time import sleep
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kafka import KafkaProducer
from faker import Faker


dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv('KAFKA_HOST')
kafka_topic = os.getenv('KAFKA_TOPIC_NAME')

producer = KafkaProducer(bootstrap_servers=f'{kafka_host}:9092')
faker = Faker()


class DataGenerator(object):
    '''
    A class for generating synthetic data.
    '''

    @staticmethod
    def get_data():
        '''
        Generate and return a list of fake data.

        Returns:
            list: A list containing fake data elements.
        '''
        now = datetime.now()
        return [
            uuid.uuid4().__str__(),
            faker.random_int(min=1, max=100),
            faker.random_element(elements=('Chair', 'Table', 'Desk', 'Sofa', 'Bed')),
            faker.safe_color_name(),
            faker.random_int(min=100, max=150000),
            faker.unix_time(start_datetime=now - timedelta(minutes=60), end_datetime=now),
        ]


while True:
    columns = [
        'order_id',
        'customer_id',
        'furniture',
        'color',
        'price',
        'ts',
    ]
    data_list = DataGenerator.get_data()
    json_data = dict(zip(columns, data_list))
    _payload = json.dumps(json_data).encode('utf-8')
    print(_payload, flush=True)
    print('=-' * 5, flush=True)
    response = producer.send(topic=kafka_topic, value=_payload)
    print(response.get())
    print('=-' * 20, flush=True)
    sleep(6)
