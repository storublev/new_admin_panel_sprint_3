"""Подключение к PostgreSQL и извлечение данных."""

import logging
import time
import functools
from typing import Any, Generator

import psycopg2
from psycopg2.extras import DictCursor

from core.config import settings

logger = logging.getLogger(__name__)


def backoff(
    start_delay: float = 1.0,
    max_delay: float = 60.0,
    max_retries: int = 10,
) -> Any:
    """Декоратор для экспоненциального backoff при ошибках подключения.

    Args:
        start_delay: Начальная задержка (сек).
        max_delay: Максимальная задержка (сек).
        max_retries: Максимальное число повторов.

    Returns:
        Результат обёрнутой функции.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = start_delay
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                    last_exc = exc
                    logger.warning(
                        "[%d/%d] Ошибка соединения: %s. Повтор через %.1f с",
                        attempt, max_retries, exc, delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)
            logger.error("Исчерпаны все попытки подключения к PostgreSQL.")
            raise last_exc  # type: ignore
        return wrapper
    return decorator


def get_pg_connection():
    """Создаёт и возвращает соединение с PostgreSQL с backoff."""
    @backoff(max_retries=10)
    def _connect():
        conn = psycopg2.connect(settings.dsn, cursor_factory=DictCursor)
        return conn
    return _connect()


def fetch_movies(cursor, last_modified: str, limit: int) -> list[dict[str, Any]]:
    """Выполняет запрос на получение фильмов, изменённых после last_modified.

    Args:
        cursor: Курсор PostgreSQL.
        last_modified: Дата в формате 'YYYY-MM-DD HH:MM:SS.mmmmmm'.
        limit: Максимальное количество записей.

    Returns:
        Список строк-словарей с данными фильмов.
    """
    from core.queries import FETCH_MODIFIED_MOVIES

    query = FETCH_MODIFIED_MOVIES.format(last_modified=last_modified, limit=limit)
    cursor.execute(query)
    return [dict(row) for row in cursor.fetchall()]


def iter_movie_batches(
    last_modified: str,
    batch_size: int = 100,
) -> Generator[list[dict[str, Any]], None, str | None]:
    """Итератор по батчам фильмов из PostgreSQL.

    Args:
        last_modified: Стартовая дата.
        batch_size: Размер батча.

    Yields:
        Очередной батч записей.

    Returns:
        Последняя дата modified из последнего батча (или None).
    """
    conn = get_pg_connection()
    try:
        with conn.cursor() as cursor:
            while True:
                rows = fetch_movies(cursor, last_modified, batch_size)
                if not rows:
                    break
                yield rows
                last_modified = rows[-1]["modified"]
    finally:
        conn.close()