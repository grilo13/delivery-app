import os

import motor.motor_asyncio

MONGODB_URL = os.getenv('MONGODB_URL', 'mongodb://admin:S3cret@localhost:27017')
DATABASE = os.getenv('MONGO_DB', 'delivery-app')

client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URL)

database = client.get_default_database(DATABASE)
