"""Подключение к Elasticsearch и загрузка данных."""

import logging
import time
import functools

from elasticsearch import Elasticsearch, helpers
from elasticsearch.exceptions import ConnectionError as ESConnectionError

from core.schemas import MOVIES_INDEX, MOVIES_INDEX_BODY
from core.config import settings

logger = logging.getLogger(__name__)


def backoff_es(
    start_delay: float = 1.0,
    max_delay: float = 60.0,
    max_retries: int = 10,
):
    """Декоратор backoff для операций с Elasticsearch."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = start_delay
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except ESConnectionError as exc:
                    last_exc = exc
                    logger.warning(
                        "[%d/%d] Elasticsearch недоступен: %s. Повтор через %.1f с",
                        attempt, max_retries, exc, delay,
                    )
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)
            logger.error("Исчерпаны все попытки подключения к Elasticsearch.")
            raise last_exc  # type: ignore
        return wrapper
    return decorator


def get_es_client() -> Elasticsearch:
    """Создаёт и возвращает клиент Elasticsearch с backoff."""

    @backoff_es(max_retries=10)
    def _connect():
        client = Elasticsearch(settings.elastic_host)
        client.info()  # Проверка доступности
        return client

    return _connect()


def ensure_index(client: Elasticsearch) -> None:
    """Создаёт индекс movies, если он ещё не существует.

    Args:
        client: Клиент Elasticsearch.
    """
    if not client.indices.exists(index=MOVIES_INDEX):
        logger.info("Создаю индекс «%s»…", MOVIES_INDEX)
        client.indices.create(index=MOVIES_INDEX, body=MOVIES_INDEX_BODY)
        logger.info("Индекс «%s» создан.", MOVIES_INDEX)
    else:
        logger.debug("Индекс «%s» уже существует.", MOVIES_INDEX)


def bulk_upload(client: Elasticsearch, documents: list[dict]) -> int:
    """Загружает пачку документов в Elasticsearch через bulk API.

    Args:
        client: Клиент Elasticsearch.
        documents: Список документов (каждый должен содержать _index, _id, _source).

    Returns:
        Количество успешно загруженных документов.
    """
    success, errors = helpers.bulk(client, documents, stats_only=True, raise_on_error=False)
    if errors:
        logger.warning("При загрузке произошло %d ошибок.", errors)
    return success