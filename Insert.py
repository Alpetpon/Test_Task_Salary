import asyncio
import motor.motor_asyncio
import bson

async def load_data_from_file():
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.2.5")
    db = client["db_sal"]
    collection = db["sal"]

    with open('sample_collection.bson', 'rb') as file:
        bson_data = file.read()
        parsed_data = bson.decode_all(bson_data)

        for doc in parsed_data:
            doc['_id'] = str(doc['_id'])
            await collection.insert_one(doc)

    print("Data loaded successfully.")

asyncio.run(load_data_from_file())