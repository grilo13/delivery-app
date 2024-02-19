import asyncio
import os
import logging

from aiokafka import AIOKafkaProducer

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', '0.0.0.0:29092')
PRODUCER_TOPIC = os.getenv('PRODUCER_TOPIC', 'teste')

logger = logging.getLogger(__name__)


async def main():
    logger.info("Producer started using bootstrap {}".format(BOOTSTRAP_SERVERS))
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()

    try:
        # Produce message
        await producer.send_and_wait(PRODUCER_TOPIC, b"message")
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()
        # await asyncio.Event().wait()


if __name__ == '__main__':
    asyncio.run(main())
