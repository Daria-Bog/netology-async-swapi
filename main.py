import asyncio
import aiohttp
from more_itertools import chunked
from models import Character, Session, engine
from tenacity import retry, stop_after_attempt, wait_fixed

CHUNK_SIZE = 10


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def fetch_json(url, session):
    async with session.get(url) as response:
        if response.status == 200:
            return await response.json()
        return None


async def get_total_characters(session):
    """Узнаем общее количество персонажей в API."""
    print("Определяем общее количество персонажей...")
    data = await fetch_json("https://www.swapi.tech/api/people", session)
    if data and "total_records" in data:
        return int(data["total_records"])
    return 100  # Запасной вариант, если API не ответил


async def get_resource_titles(urls, session):
    """Получает названия ресурсов. Возвращает пустую строку, если данных нет."""
    if not urls:
        return ""
    if isinstance(urls, str):
        urls = [urls]

    # Чтобы ошибка в одном запросе не ломала всё, обрабатываем результаты аккуратно
    tasks = [fetch_json(url, session) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    titles = []
    for res in results:
        if isinstance(res, dict) and "result" in res:
            props = res["result"]["properties"]
            titles.append(props.get("name") or props.get("title"))
    return ", ".join(filter(None, titles))


async def get_person(person_id, session):
    url = f"https://www.swapi.tech/api/people/{person_id}"
    data = await fetch_json(url, session)
    if not data or "result" not in data:
        return None

    props = data["result"]["properties"]

    # Запускаем все доп. запросы параллельно для ускорения
    homeworld_task = get_resource_titles(props.get("homeworld"), session)
    films_task = get_resource_titles(props.get("films"), session)
    species_task = get_resource_titles(props.get("species"), session)
    starships_task = get_resource_titles(props.get("starships"), session)
    vehicles_task = get_resource_titles(props.get("vehicles"), session)

    titles = await asyncio.gather(
        homeworld_task, films_task, species_task, starships_task, vehicles_task
    )

    return {
        "id": int(data["result"]["uid"]),
        "name": props.get("name"),
        "birth_year": props.get("birth_year"),
        "eye_color": props.get("eye_color"),
        "gender": props.get("gender"),
        "hair_color": props.get("hair_color"),
        "homeworld": titles[0],
        "mass": props.get("mass"),
        "skin_color": props.get("skin_color"),
        "films": titles[1],
        "species": titles[2],
        "starships": titles[3],
        "vehicles": titles[4],
    }


async def insert_to_db(characters_data):
    async with Session() as session:
        # Используем merge вместо add для идемпотентности (если ID уже есть, он обновится, а не выдаст ошибку)
        for item in characters_data:
            if item:
                char = Character(**item)
                await session.merge(char)
        await session.commit()
    print(f"Обработано и сохранено: {len(characters_data)} персонажей")


async def main():
    db_tasks = []
    async with aiohttp.ClientSession() as http_session:
        # Динамически определяем количество персонажей
        total_chars = await get_total_characters(http_session)
        print(f"Всего персонажей для загрузки: {total_chars}")

        for char_ids_chunk in chunked(range(1, total_chars + 1), CHUNK_SIZE):
            tasks = [get_person(cid, http_session) for cid in char_ids_chunk]
            results = await asyncio.gather(*tasks)
            valid_results = [r for r in results if r]

            if valid_results:
                task = asyncio.create_task(insert_to_db(valid_results))
                db_tasks.append(task)

    if db_tasks:
        await asyncio.gather(*db_tasks)

    await engine.dispose()
    print("Работа завершена!")


if __name__ == "__main__":
    asyncio.run(main())