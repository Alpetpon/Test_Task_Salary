import logging
import json
import motor.motor_asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from datetime import datetime, timedelta


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

    current_date = dt_from
    date_range = []

    while current_date <= dt_upto:
        if group_type == 'month':
            date_range.append(current_date.strftime('%Y-%m-01'))
            current_date += timedelta(days=31)
        elif group_type == 'day':
            date_range.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        elif group_type == 'hour':
            date_range.append(current_date.strftime('%Y-%m-%dT%H:00:00'))
            current_date += timedelta(hours=1)

    final_result = {'dataset': [], 'labels': []}
    for date in date_range:
        found = False
        formatted_date = datetime.fromisoformat(date).strftime('%Y-%m-%dT%H:%M:%S')
        final_result['labels'].append(formatted_date)
        for data in result:
            if data['labels'] == date:
                final_result['dataset'].append(data['dataset'])
                found = True
                break
        if not found:
            final_result['dataset'].append(0)

    return final_result



logging.basicConfig(level=logging.INFO)



bot = Bot(token="6609110157:AAH9pF91XMl3z68T4ALdawNj5wToRVjlaAQ")
dispatcher = Dispatcher()

@dispatcher.message(Command(commands=['start']))
async def send_welcome(message: types.Message):
    user_mention = f'<a href="tg://user?id={message.from_user.id}">{message.from_user.first_name} {message.from_user.last_name}</a>'
    greeting_message = f"Hi {user_mention}!"
    await message.reply( greeting_message, parse_mode='HTML')

@dispatcher.message()
async def process_message(message: types.Message):
    try:
        data = json.loads(message.text)
        dt_from = datetime.fromisoformat(data["dt_from"])
        dt_upto = datetime.fromisoformat(data["dt_upto"])
        group_type = data["group_type"]

        result = await aggregate_data(dt_from, dt_upto, group_type)
        formatted_result = json.dumps(result, indent=4)
        await message.reply(formatted_result)
    except Exception as e:
        await message.reply(f"Error processing message: {e}")


if __name__ == '__main__':
    dispatcher.run_polling(bot)