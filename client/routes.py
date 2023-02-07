import os
import uuid

from fastapi import APIRouter
from schemas import Order
from database import database

from aiokafka import AIOKafkaProducer

BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', '0.0.0.0:29092')
PRODUCER_TOPIC = os.getenv('PRODUCER_TOPIC', 'teste')

router = APIRouter()


@router.post("/order", response_description="Request an order")
async def receive_order(order: Order):
    print("New order received")
    print(order)

    collection = database['order']
    order_obj = order.dict()
    order_obj.update({'order_id': str(uuid.uuid4().hex)})

    print("Saving the order to the DB")
    try:
        collection.insert_one(order_obj)
    except Exception as e:
        print("DB is not available.. error {}".format(e.__str__()))

    # Push an order to memphis
    print("Pushing order to processing")
    # kafka producer
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()

    try:
        # Produce message
        order_obj.pop('_id')
        await producer.send_and_wait(PRODUCER_TOPIC, str(order_obj).encode('utf-8'))
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()
        # await asyncio.Event().wait()

    return 200
