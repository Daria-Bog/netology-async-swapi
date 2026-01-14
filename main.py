import asyncio
import aiohttp
from more_itertools import chunked
from models import Character, Session, init_db, engine
from tenacity import retry, stop_after_attempt, wait_fixed

MAX_CHARACTERS = 100
CHUNK_SIZE = 10


# Декоратор для повторных попыток при сбое сети
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def fetch_json(url, session):
    async with session.get(url) as response:
        if response.status == 200:
            return await response.json()
        return None


async def get_resource_titles(urls, session, field_name="name"):
    """Получает названия ресурсов (фильмов, кораблей и т.д.) по списку URL."""
    if not urls:
        return ""
    if isinstance(urls, str):  # Для homeworld, где только один URL
        urls = [urls]

    tasks = [fetch_json(url, session) for url in urls]
    results = await asyncio.gather(*tasks)

    titles = []
    for res in results:
        if res and "result" in res:
            # В SWAPI названия лежат либо в "name", либо в "title" (для фильмов)
            props = res["result"]["properties"]
            titles.append(props.get("name") or props.get("title"))

    return ", ".join(filter(None, titles))


async def get_person(person_id, session):
    url = f"https://www.swapi.tech/api/people/{person_id}"
    data = await fetch_json(url, session)

    if not data or "result" not in data:
        return None

    props = data["result"]["properties"]

    # Собираем данные и сразу запрашиваем названия вместо ссылок
    homeworld_title = await get_resource_titles(props.get("homeworld"), session)
    films_titles = await get_resource_titles(props.get("films"), session, "title")
    species_titles = await get_resource_titles(props.get("species"), session)
    starships_titles = await get_resource_titles(props.get("starships"), session)
    vehicles_titles = await get_resource_titles(props.get("vehicles"), session)

    return {
        "id": int(data["result"]["uid"]),
        "name": props.get("name"),
        "birth_year": props.get("birth_year"),
        "eye_color": props.get("eye_color"),
        "gender": props.get("gender"),
        "hair_color": props.get("hair_color"),
        "homeworld": homeworld_title,
        "mass": props.get("mass"),
        "skin_color": props.get("skin_color"),
        "films": films_titles,
        "species": species_titles,
        "starships": starships_titles,
        "vehicles": vehicles_titles,
    }


async def insert_to_db(characters_data):
    async with Session() as session:
        objects = [Character(**item) for item in characters_data if item]
        session.add_all(objects)
        await session.commit()
    print(f"Записано в базу: {len(objects)} персонажей")


async def main():
    await init_db()
    db_tasks = []  # Список для контроля задач БД

    async with aiohttp.ClientSession() as http_session:
        for char_ids_chunk in chunked(range(1, MAX_CHARACTERS + 1), CHUNK_SIZE):
            tasks = [get_person(cid, http_session) for cid in char_ids_chunk]
            results = await asyncio.gather(*tasks)
            valid_results = [r for r in results if r]

            if valid_results:
                task = asyncio.create_task(insert_to_db(valid_results))
                db_tasks.append(task)

    # Ждем только наши задачи по записи в БД
    if db_tasks:
        await asyncio.gather(*db_tasks)

    await engine.dispose()
    print("Выгрузка завершена успешно!")


if __name__ == "__main__":
    asyncio.run(main())