"""ETL-процесс: перенос данных из PostgreSQL в Elasticsearch."""

import logging
import sys
import time
from typing import Optional

from core.config import settings
from db.postgres import iter_movie_batches, count_modified_movies
from db.elastic import get_es_client, ensure_index, bulk_upload_with_progress
from models.dataclasses import Movie, Person, Genre
from state import JsonFileStorage, State

logger = logging.getLogger(__name__)

STATE_KEY = "films_last_modified"


def configure_logging() -> None:
    """Настраивает формат и уровень логирования."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def transform_row(row: dict) -> Movie:
    """Преобразует строку из БД в объект Movie."""
    actors: list[Person] = []
    directors: list[Person] = []
    writers: list[Person] = []

    for person in row.get("persons", []):
        p = Person(id=person["id"], name=person["name"])
        role = person.get("role", "")
        if role == "actor":
            actors.append(p)
        elif role == "director":
            directors.append(p)
        elif role == "writer":
            writers.append(p)

    genres = [
        Genre(id=g["id"], name=g["name"])
        for g in row.get("genres", [])
    ]

    return Movie(
        id=row["id"],
        title=row["title"],
        description=row.get("description"),
        imdb_rating=row.get("imdb_rating"),
        creation_date=row.get("creation_date"),
        genres=genres,
        actors=actors,
        directors=directors,
        writers=writers,
    )


def to_bulk_action(movie: Movie) -> dict:
    """Формирует bulk-действие для Elasticsearch."""
    return {
        "_index": "movies",
        "_id": movie.id,
        "_source": movie.to_es_document(),
    }


def run_etl() -> None:
    """Запускает ETL-процесс."""
    logger.info("🚀 Запуск ETL-процесса…")

    storage = JsonFileStorage(settings.state_file)
    state = State(storage)

    cycle_count = 0

    while True:
        cycle_count += 1
        logger.info(f"🔄 Цикл ETL #{cycle_count}")

        try:
            # Проверяем подключения
            logger.info("🔌 Подключение к Elasticsearch...")
            es = get_es_client()
            ensure_index(es)
            logger.info("✅ Подключение к Elasticsearch установлено")

            # Получаем дату последней загрузки
            last_modified = state.get(STATE_KEY, "1970-01-01 00:00:00.000000")
            logger.info(f"📅 Загружаю фильмы, изменённые после {last_modified}")

            # Проверяем, есть ли новые данные
            count = count_modified_movies(last_modified)
            logger.info(f"📊 Найдено {count} новых/изменённых фильмов")

            if count == 0:
                logger.info("⏸️ Новых данных нет, ожидаем...")
                time.sleep(settings.poll_interval)
                continue

            # Получаем генератор батчей
            batch_generator = iter_movie_batches(last_modified, settings.batch_size)

            processed_count = 0
            new_last_modified = last_modified

            # Обрабатываем батчи
            for batch_index, batch in enumerate(batch_generator, 1):
                logger.info(f"📦 Обрабатываю батч #{batch_index} из {len(batch)} записей")

                # Трансформация данных
                transformed = [transform_row(row) for row in batch]
                bulk_actions = [to_bulk_action(m) for m in transformed]

                # Загрузка в Elasticsearch
                success_count = bulk_upload_with_progress(es, bulk_actions, batch_size=500)
                processed_count += success_count

                logger.info(f"✅ Загружено {success_count} документов. Всего: {processed_count}/{count}")

            # Сохраняем состояние
            if processed_count > 0:
                # Получаем финальную дату из итератора
                final_date = getattr(batch_generator, 'gi_frame', None)
                if final_date:
                    # Если итератор вернул значение
                    try:
                        final_modified = next(batch_generator, None)
                        if final_modified:
                            new_last_modified = final_modified
                    except StopIteration:
                        pass

                state.set(STATE_KEY, new_last_modified)
                logger.info(f"💾 Состояние сохранено: {new_last_modified}")
                logger.info(f"✅ Обработано {processed_count} фильмов")
            else:
                logger.warning("⚠️ Не обработано ни одного фильма")

        except Exception as exc:
            logger.error(f"❌ Ошибка в ETL-цикле: {exc}", exc_info=True)
            logger.info(f"⏳ Повторная попытка через {settings.poll_interval} с…")
            time.sleep(settings.poll_interval * 2)
            continue

        # Пауза перед следующим циклом
        logger.info(f"⏳ Ожидание {settings.poll_interval} с перед следующим циклом...")
        time.sleep(settings.poll_interval)


def main() -> None:
    """Точка входа."""
    configure_logging()
    try:
        run_etl()
    except KeyboardInterrupt:
        logger.info("🛑 ETL остановлен пользователем.")


if __name__ == "__main__":
    main()