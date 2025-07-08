from fastapi import FastAPI
import redis.asyncio as redis
import json
import os
from typing import Optional, Any

app = FastAPI(title="Hello API", version="1.0.0")

# Получаем параметры подключения к Redis из переменных окружения
REDIS_HOST = os.getenv('REDIS_HOST', '51.250.42.45')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6380))
REDIS_USERNAME = os.getenv('REDIS_USERNAME', 'admin')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', '%Qb9OyqXABDWeCv*')
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# Создаем подключение к Redis
redis_client = redis.Redis(
    host=REDIS_HOST, 
    username=REDIS_USERNAME, 
    password=REDIS_PASSWORD, 
    port=REDIS_PORT, 
    db=REDIS_DB, 
    decode_responses=True
)

async def ping_redis():
    print(await redis_client.ping(), 'ping')
    return {'status': 'ok'}

@app.get("/ping")
async def ping():
    return await ping_redis()

async def get_cards_from_redis() -> Optional[Any]:
    """
    Асинхронная функция для извлечения записи из Redis по ключу 'cards'
    
    Returns:
        Optional[Any]: Данные из Redis или None, если ключ не найден
    """
    try:
        # Извлекаем данные по ключу 'cards'
        data = await redis_client.lrange('cards', 0, -1) # type: ignore
        print(data, 'data')
        if not data:  # Check for empty list instead of None
            return None
        
        # Parse each JSON string in the array
        parsed_data = []
        for item in data:
            try:
                parsed_item = json.loads(item)
                parsed_data.append(parsed_item)
            except json.JSONDecodeError:
                # If not valid JSON, keep as string
                parsed_data.append(item)
        
        return parsed_data
            
    except redis.RedisError as e:
        print(f"Ошибка при работе с Redis: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        return None

@app.get("/cards")
async def get_cards():
    """Эндпоинт для получения карточек из Redis"""
    cards_data = await get_cards_from_redis()
    
    if cards_data is None:
        return []
    
    return cards_data
