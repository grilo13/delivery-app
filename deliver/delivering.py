import asyncio
import os
import logging

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from client.database import database

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', '0.0.0.0:29092')
DELIVERY_TOPIC = os.getenv('DELIVERY_TOPIC', 'delivery')

logger = logging.getLogger(__name__)


async def consume():
    logger.info("Starting to consume on {}".format(BOOTSTRAP_SERVERS))
    consumer = AIOKafkaConsumer(
        DELIVERY_TOPIC,
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

            order.update({'status': 'finished'})

            try:
                update_values = {"$set": {'status': "finished"}}
                collection.update_one({'order_id': order["order_id"]}, update_values)
            except Exception as err:
                print("DB is not available.. error {}".format(err.__str__()))

    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


if __name__ == '__main__':
    asyncio.run(consume())
