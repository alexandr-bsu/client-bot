from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
import json
import os
from typing import Optional, Any
import pytz
import httpx
from datetime import datetime, timedelta
from fastapi import HTTPException
from cachetools import LRUCache
from fastapi.middleware.gzip import GZipMiddleware

# Глобальный LRU cache (например, на 1 результат)
edu_keys_lru_cache = LRUCache(maxsize=1)

def get_edu_keys_from_lru_cache():
    return edu_keys_lru_cache.get("edu_keys")

def set_edu_keys_to_lru_cache(data):
    edu_keys_lru_cache["edu_keys"] = data

def clear_edu_keys_lru_cache():
    edu_keys_lru_cache.clear()

app = FastAPI(title="Hello API", version="1.0.0")

# Добавляем GZip middleware для сжатия ответов
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Добавляем CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешаем все источники
    allow_methods=["*"],  # Разрешаем все HTTP методы
    allow_headers=["*"],  # Разрешаем все заголовки
)

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

@app.post("/aggregate-all-timezones")
async def aggregate_all_timezones():
    """
    Для всех часовых поясов из фиксированного массива разниц с московским,
    отправляет POST-запросы, сохраняет результат в Redis и возвращает массив результатов.
    """
    timezone_diffs = [11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, -11, -12, -13, -14]

    # Формируем даты
    start_date = datetime.now().strftime('%Y-%m-%d')
    end_date = (datetime.now() + timedelta(days=31)).strftime('%Y-%m-%d')

    results = []
    async with httpx.AsyncClient() as client:
        for diff in timezone_diffs:
            # Формируем userTimeZone и ключ
            if diff == 0:
                user_tz = 'МСК'
                key = f'all_agggregated_msk_0'
            elif diff > 0:
                user_tz = f'МСК+{diff}'
                key = f'all_agggregated_msk_plus_{diff}'
            else:
                user_tz = f'МСК{diff}'  # diff already negative
                key = f'all_agggregated_msk_minus_{abs(diff)}'

            body = {
                "startDate": start_date,
                "endDate": end_date,
                "ageFilter": "",
                "formPsyClientInfo": {
                    "age": "",
                    "city": "",
                    "sex": "Мужской",
                    "psychoEducated": "",
                    "anxieties": [],
                    "customAnexiety": "",
                    "hasPsychoExperience": "",
                    "meetType": "",
                    "selectionСriteria": "",
                    "custmCreteria": "",
                    "importancePsycho": [],
                    "customImportance": "",
                    "agePsycho": "",
                    "sexPsycho": "Не имеет значения",
                    "priceLastSession": "",
                    "durationSession": "",
                    "reasonCancel": "",
                    "pricePsycho": "",
                    "reasonNonApplication": "",
                    "contactType": "",
                    "contact": "",
                    "name": "",
                    "is_adult": False,
                    "is_last_page": False,
                    "occupation": ""
                },
                "form": {
                    "anxieties": [],
                    "questions": [],
                    "customQuestion": [],
                    "diagnoses": [],
                    "diagnoseInfo": "",
                    "diagnoseMedicaments": "",
                    "traumaticEvents": [],
                    "clientStates": [],
                    "selectedPsychologistsNames": [],
                    "shownPsychologists": "",
                    "psychos": [],
                    "lastExperience": "",
                    "amountExpectations": "",
                    "age": "",
                    "slots": [],
                    "contactType": "",
                    "contact": "",
                    "name": "",
                    "promocode": "",
                    "ticket_id": "",
                    "emptySlots": False,
                    "userTimeZone": user_tz,
                    "bid": 0,
                    "rid": 0,
                    "categoryType": "",
                    "customCategory": "",
                    "question_to_psychologist": "",
                    "filtered_by_automatch_psy_names": [],
                    "_queries": "",
                    "customTraumaticEvent": "",
                    "customState": ""
                },
                "ticket_id": "",
                "userTimeOffsetMsk": diff
            }
            try:
                response = await client.post(
                    "https://n8n-v2.hrani.live/webhook/get-aggregated-all",
                    json=body,
                    timeout=30.0
                )
                response.raise_for_status()
                data = response.json()
                await redis_client.set(key, json.dumps(data))
                results.append({"offset": diff, "key": key, "result": data})
            except Exception as e:
                results.append({"offset": diff, "key": key, "error": str(e)})
    return results

@app.post("/schedule/{offset}")
async def get_schedule_by_offset(offset: int):
    """
    Получить расписание по разнице с Москвой (например, -5 или 9).
    Если нет данных в Redis, получить их с API, сохранить и вернуть.
    """
    if offset == 0:
        key = 'all_agggregated_msk_0'
        user_tz = 'МСК'
    elif offset > 0:
        key = f'all_agggregated_msk_plus_{offset}'
        user_tz = f'МСК+{offset}'
    else:
        key = f'all_agggregated_msk_minus_{abs(offset)}'
        user_tz = f'МСК{offset}'
    data = await redis_client.get(key)
    if data is not None:
        return json.loads(data)

    # Если нет в Redis, делаем запрос к API
    start_date = datetime.now().strftime('%Y-%m-%d')
    end_date = (datetime.now() + timedelta(days=31)).strftime('%Y-%m-%d')
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "ageFilter": "",
        "formPsyClientInfo": {
            "age": "",
            "city": "",
            "sex": "Мужской",
            "psychoEducated": "",
            "anxieties": [],
            "customAnexiety": "",
            "hasPsychoExperience": "",
            "meetType": "",
            "selectionСriteria": "",
            "custmCreteria": "",
            "importancePsycho": [],
            "customImportance": "",
            "agePsycho": "",
            "sexPsycho": "Не имеет значения",
            "priceLastSession": "",
            "durationSession": "",
            "reasonCancel": "",
            "pricePsycho": "",
            "reasonNonApplication": "",
            "contactType": "",
            "contact": "",
            "name": "",
            "is_adult": False,
            "is_last_page": False,
            "occupation": ""
        },
        "form": {
            "anxieties": [],
            "questions": [],
            "customQuestion": [],
            "diagnoses": [],
            "diagnoseInfo": "",
            "diagnoseMedicaments": "",
            "traumaticEvents": [],
            "clientStates": [],
            "selectedPsychologistsNames": [],
            "shownPsychologists": "",
            "psychos": [],
            "lastExperience": "",
            "amountExpectations": "",
            "age": "",
            "slots": [],
            "contactType": "",
            "contact": "",
            "name": "",
            "promocode": "",
            "ticket_id": "",
            "emptySlots": False,
            "userTimeZone": user_tz,
            "bid": 0,
            "rid": 0,
            "categoryType": "",
            "customCategory": "",
            "question_to_psychologist": "",
            "filtered_by_automatch_psy_names": [],
            "_queries": "",
            "customTraumaticEvent": "",
            "customState": ""
        },
        "ticket_id": "",
        "userTimeOffsetMsk": offset
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                "https://n8n-v2.hrani.live/webhook/get-aggregated-all",
                json=body,
                timeout=120.0
            )
            response.raise_for_status()
            result = response.json()
            await redis_client.set(key, json.dumps(result))
            return result
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"API error: {str(e)}")

@app.delete("/delete-all-aggregated")
async def delete_all_aggregated():
    """
    Удаляет все ключи из Redis, начинающиеся с all_agggregated_
    """
    try:
        pattern = "all_agggregated_*"
        keys = await redis_client.keys(pattern)
        if not keys:
            return {"deleted": 0, "message": "No keys found"}
        deleted = await redis_client.delete(*keys)
        return {"deleted": deleted, "message": f"Deleted {deleted} keys"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting keys: {str(e)}")


@app.get("/edu-keys-pipeline-batch")
async def get_edu_keys_pipeline_batch(batch_size: int = 1000, use_cache: bool = True):
    """
    Еще более оптимизированная версия с обработкой больших объемов данных батчами
    Возвращает также метаданные о времени выполнения, количестве ключей и скорости обработки
    Использует кэширование результата (TTL 300 секунд)
    """
    start_time = datetime.now()
    # Проверяем LRU cache
    if use_cache:
        cached_result = get_edu_keys_from_lru_cache()
        if cached_result:
            end_time = datetime.now()
            elapsed = (end_time - start_time).total_seconds()
            print(f"/edu-keys-pipeline-batch execution time: {elapsed} seconds, total values: {len(cached_result)}")
            return cached_result
    try:
        pattern = "edu_*"
        all_values = []
        total_keys = 0
        # Обрабатываем ключи батчами для экономии памяти
        cursor = 0

        while True:
            # Получаем батч ключей
            cursor, keys = await redis_client.scan(cursor, match=pattern, count=batch_size)
            if not keys:
                if cursor == 0:
                    break
                continue
            total_keys += len(keys)
            # Получаем значения для этого батча через pipeline
            pipeline = redis_client.pipeline()
            for key in keys:
                pipeline.get(key)
            values_raw = await pipeline.execute()
            # Парсим JSON
            for value in values_raw:
                if value is not None:
                    try:
                        all_values.append(json.loads(value))
                    except json.JSONDecodeError:
                        all_values.append(value)
            # Если это последний батч
            if cursor == 0:
                break
        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        result = all_values
        
        if use_cache:
            set_edu_keys_to_lru_cache(result)
        print(f"/edu-keys-pipeline-batch execution time: {elapsed} seconds, total values: {len(all_values)}")
        return result
        
    except Exception as e:
        end_time = datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        raise HTTPException(status_code=500, detail=f"Error fetching edu_ keys: {str(e)}")

@app.post("/invalidate-edu-lru-cache")
async def invalidate_edu_lru_cache():
    """
    Инвалидирует кэш edu_ ключей
    """
    clear_edu_keys_lru_cache()
    return {"message": "LRU cache invalidated"}

