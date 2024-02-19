import asyncio
import os
import logging
import time
import random

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from client.database import database
from datetime import datetime

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', '0.0.0.0:29092')
CONSUMER_TOPIC = os.getenv('CONSUMER_TOPIC', 'request')
DELIVERY_TOPIC = os.getenv('DELIVERY_TOPIC', 'delivery')

logger = logging.getLogger(__name__)


async def consume():
    logger.info("Starting to consume on {}".format(BOOTSTRAP_SERVERS))
    consumer = AIOKafkaConsumer(
        CONSUMER_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None)

    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            order = msg.value.decode('utf-8')
            order = eval(order)
            collection = database['order']
            current_datetime = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

            order.update({'status': 'processed',
                          'update_date': current_datetime})

            try:
                time.sleep(random.randint(0, 5))
                update_values = {"$set": {'status': "processed", 'update_date': current_datetime}}
                collection.update_one({'order_id': order["order_id"]}, update_values)
            except Exception as err:
                print("DB is not available.. error {}".format(err.__str__()))

            producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
            # Get cluster layout and initial topic/partition leadership information
            await producer.start()

            try:
                # Produce message
                await producer.send_and_wait(DELIVERY_TOPIC, str(order).encode('utf-8'))
            finally:
                # Wait for all pending messages to be delivered or expire.
                await producer.stop()
                # await asyncio.Event().wait()
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(consume())
