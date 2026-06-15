"""ETL-процесс: перенос данных из PostgreSQL в Elasticsearch.

Основной цикл:
  1. Подключение к PostgreSQL и Elasticsearch.
  2. Чтение состояния (даты последней загрузки).
  3. Извлечение фильмов из PostgreSQL (батчами по batch_size).
  4. Трансформация строк в формат документов Elasticsearch.
  5. Загрузка в Elasticsearch через bulk API.
  6. Обновление состояния.
  7. Ожидание poll_interval секунд, затем повтор.
"""

import logging
import sys
import time

from core.config import settings
from db.postgres import iter_movie_batches
from db.elastic import get_es_client, ensure_index, bulk_upload
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
    """Преобразует строку из БД в объект Movie.

    Args:
        row: Словарь с данными из PostgreSQL.

    Returns:
        Объект Movie с разделёнными персонами по ролям.
    """
    actors: list[Person] = []
    directors: list[Person] = []
    writers: list[Person] = []

    # Разделяем персон по ролям
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
    """Формирует bulk-действие для Elasticsearch.

    Args:
        movie: Объект фильма.

    Returns:
        Словарь для helpers.bulk().
    """
    return {
        "_index": "movies",
        "_id": movie.id,
        "_source": movie.to_es_document(),
    }


def run_etl() -> None:
    """Запускает ETL-процесс.

    В бесконечном цикле:
      - проверяет подключения,
      - читает новые/изменённые фильмы из PostgreSQL,
      - загружает их в Elasticsearch.
    """
    logger.info("Запуск ETL-процесса…")

    storage = JsonFileStorage(settings.state_file)
    state = State(storage)

    while True:
        try:
            # Проверяем подключения
            es = get_es_client()
            ensure_index(es)

            last_modified = state.get(STATE_KEY, "1970-01-01 00:00:00.000000")
            logger.info("Загружаю фильмы, изменённые после %s", last_modified)

            # Итерируемся по батчам
            batch_generator = iter_movie_batches(last_modified, settings.batch_size)
            new_last_modified = last_modified

            for batch in batch_generator:
                logger.info("Обрабатываю батч из %d записей…", len(batch))

                transformed = [transform_row(row) for row in batch]
                bulk_actions = [to_bulk_action(m) for m in transformed]

                success_count = bulk_upload(es, bulk_actions)
                logger.info("Загружено %d документов.", success_count)

                # Сохраняем дату последнего обработанного фильма
                new_last_modified = batch[-1]["modified"]
                state.set(STATE_KEY, new_last_modified)

            logger.info(
                "Цикл завершён. Следующая проверка через %d с.",
                settings.poll_interval,
            )

        except Exception as exc:
            logger.error("Ошибка в ETL-цикле: %s", exc, exc_info=True)
            logger.info("Повторная попытка через %d с…", settings.poll_interval)

        time.sleep(settings.poll_interval)


def main() -> None:
    """Точка входа."""
    configure_logging()
    try:
        run_etl()
    except KeyboardInterrupt:
        logger.info("ETL остановлен пользователем.")


if __name__ == "__main__":
    main()