import asyncio
import motor.motor_asyncio

async def print_collection():
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.2.5")
    db = client["db_sal"]
    collection = db["sal"]

    async for document in collection.find():
        print(document)

asyncio.run(print_collection())