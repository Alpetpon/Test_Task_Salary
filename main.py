import asyncio
import logging
import json
import motor.motor_asyncio



async def aggregate_data(dt_from, dt_upto, group_type):
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.2.5")
    db = client["db_sal"]
    collection = db["sal"]

    pipeline = [
        {
            '$match': {
                'dt': {'$gte': dt_from, '$lte': dt_upto}
            }
        },
        {
            '$project': {
                'date_group': {
                    '$switch': {
                        'branches': [
                            {'case': {'$eq': [group_type, 'month']}, 'then': {'$dateToString': {'format': '%Y-%m-01', 'date': '$dt'}}},
                            {'case': {'$eq': [group_type, 'day']}, 'then': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$dt'}}},
                            {'case': {'$eq': [group_type, 'hour']}, 'then': {'$dateToString': {'format': '%Y-%m-%dT%H:00:00', 'date': '$dt'}}}
                        ],
                        'default': 'Unknown'
                    }
                },
                'value': 1
            }
        },
        {
            '$group': {
                '_id': '$date_group',
                'dataset': {'$sum': '$value'}
            }
        },
        {
            '$project': {
                '_id': 0,
                'dataset': 1,
                'labels': '$_id'
            }
        },
        {
            '$sort': {'labels': 1}
        }
    ]

    result = await collection.aggregate(pipeline).to_list(length=None)



