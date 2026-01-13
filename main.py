import asyncio
import aiohttp
from more_itertools import chunked
from models import Character, Session, init_db, engine

# Константы для настройки выгрузки
MAX_CHARACTERS = 100
CHUNK_SIZE = 10


async def get_person(person_id, session):
    """Выгрузка данных одного персонажа из API."""
    url = f"https://www.swapi.tech/api/people/{person_id}"
    async with session.get(url) as response:
        if response.status != 200:
            return None
        data = await response.json()
        if "result" not in data:
            return None

        props = data["result"]["properties"]
        return {
            "id": int(data["result"]["uid"]),
            "name": props.get("name"),
            "birth_year": props.get("birth_year"),
            "eye_color": props.get("eye_color"),
            "gender": props.get("gender"),
            "hair_color": props.get("hair_color"),
            "homeworld": props.get("homeworld"),
            "mass": props.get("mass"),
            "skin_color": props.get("skin_color"),
        }


async def insert_to_db(characters_data):
    """Асинхронное сохранение пачки персонажей в базу данных."""
    async with Session() as session:
        objects = [Character(**item) for item in characters_data if item]
        session.add_all(objects)
        await session.commit()
    print(f"Записано в базу: {len(objects)} персонажей")


async def main():
    # Инициализация таблиц в БД
    await init_db()

    async with aiohttp.ClientSession() as http_session:
        # Разбиваем диапазон ID на чанки для параллельных запросов
        for char_ids_chunk in chunked(range(1, MAX_CHARACTERS + 1), CHUNK_SIZE):
            tasks = [get_person(cid, http_session) for cid in char_ids_chunk]
            results = await asyncio.gather(*tasks)

            # Фильтруем пустые результаты (если персонаж с таким ID не найден)
            valid_results = [r for r in results if r]

            if valid_results:
                # Запускаем запись в БД как фоновую задачу
                asyncio.create_task(insert_to_db(valid_results))

    # Ждем завершения всех запущенных задач insert_to_db перед закрытием
    all_tasks = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*all_tasks)

    # Закрываем соединение с движком БД
    await engine.dispose()
    print("Выгрузка успешно завершена!")


if __name__ == "__main__":
    asyncio.run(main())