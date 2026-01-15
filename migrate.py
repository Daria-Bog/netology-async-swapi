import asyncio
from models import engine, Base

async def migrate():
    print("Инициализация базы данных...")
    async with engine.begin() as conn:
        # Мы НЕ используем drop_all, чтобы не удалять данные
        # run_sync(Base.metadata.create_all) внутри себя уже содержит проверку "IF NOT EXISTS"
        await conn.run_sync(Base.metadata.create_all)
    print("Таблицы успешно созданы или уже существуют.")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(migrate())