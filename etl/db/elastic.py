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


# def bulk_upload(client: Elasticsearch, documents: list[dict]) -> int:
#     """Загружает пачку документов в Elasticsearch через bulk API.
#
#     Args:
#         client: Клиент Elasticsearch.
#         documents: Список документов (каждый должен содержать _index, _id, _source).
#
#     Returns:
#         Количество успешно загруженных документов.
#     """
#     if not documents:
#         logger.info("Нет параметра: documents")
#         return 0
#
#     # Лог первого документа для отладки
#     logger.info(f"Пример документа: {documents[0]['_source']}")
#
#     errors: list[dict] = []
#     success = 0
#     for ok, result in helpers.streaming_bulk(
#         client, documents, raise_on_error=False,
#     ):
#         if ok:
#             success += 1
#         else:
#             errors.append(result)
#             if len(errors) <= 5:
#                 logger.warning(f"Ошибка вставки: {result}")
#
#     if errors:
#         logger.warning(
#             f"Загрузка: успешно={success}, всего ошибок={len(errors)}",
#         )
#     else:
#         logger.info(f"Загружено {success} документов.")
#     return success

def bulk_upload(client: Elasticsearch, documents: list[dict], timeout: int = 300) -> int:
    """Загружает пачку документов в Elasticsearch через bulk API с таймаутом."""

    if not documents:
        logger.info("Нет документов для загрузки")
        return 0

    # Логирование первых документов
    for i, doc in enumerate(documents[:3]):
        logger.info(f"Документ {i + 1}: {doc['_source']}")

    errors: list[dict] = []
    success = 0
    total = len(documents)

    logger.info(f"Начинаю загрузку {total} документов...")
    start_time = time.time()

    try:
        for idx, (ok, result) in enumerate(helpers.streaming_bulk(
                client,
                documents,
                raise_on_error=False,
                chunk_size=500,
                request_timeout=timeout,  # Добавляем таймаут
                max_retries=3,  # Ограничиваем ретраи
        ), 1):

            # Проверка на превышение времени выполнения
            if time.time() - start_time > timeout:
                logger.warning(f"Превышен таймаут {timeout}с. Загружено {success}/{total}")
                break

            if ok:
                success += 1
                if success % 100 == 0:
                    logger.info(f"Загружено {success}/{total} документов")
            else:
                errors.append(result)
                if len(errors) <= 10:
                    error_doc = documents[idx - 1] if idx <= len(documents) else None
                    logger.error(f"Ошибка вставки документа #{idx}: {result}")
                    if error_doc:
                        logger.error(f"Проблемный документ ID: {error_doc.get('_id')}")

    except Exception as e:
        logger.error(f"Критическая ошибка при загрузке: {e}")
        logger.info(f"Успешно загружено: {success} документов")
        return success

    # Финальный отчет
    duration = time.time() - start_time
    if errors:
        logger.warning(
            f"Загрузка завершена за {duration:.1f}с: "
            f"✅ успешно={success}, ❌ ошибок={len(errors)}, 📊 всего={total}"
        )
    else:
        logger.info(f"✅ Все {success} документов успешно загружены за {duration:.1f}с")

    return success


def bulk_upload_with_progress(client: Elasticsearch, documents: list[dict], batch_size: int = 1000) -> int:
    """Загружает документы пакетами с детальным прогрессом."""

    if not documents:
        logger.info("Нет документов для загрузки")
        return 0

    total = len(documents)
    logger.info(f"📦 Подготовка к загрузке {total} документов")

    # Логирование первых документов
    sample_size = min(3, total)
    for i in range(sample_size):
        doc_source = documents[i]['_source']
        logger.info(f"📄 Пример документа {i + 1}: {doc_source.get('title', 'Без названия')} "
                    f"(ID: {documents[i].get('_id')})")

    success = 0
    errors = []
    start_time = time.time()

    # Разбиваем на чанки для лучшего контроля
    for batch_idx in range(0, total, batch_size):
        batch = documents[batch_idx:batch_idx + batch_size]
        logger.info(f"🔄 Загрузка пакета {batch_idx // batch_size + 1}/{(total + batch_size - 1) // batch_size} "
                    f"({len(batch)} документов)...")

        try:
            batch_success = 0
            for ok, result in helpers.streaming_bulk(
                    client,
                    batch,
                    raise_on_error=False,
                    request_timeout=120,
            ):
                if ok:
                    batch_success += 1
                    success += 1
                else:
                    errors.append(result)

            logger.info(f"📊 Пакет загружен: ✅ {batch_success}, ❌ {len(batch) - batch_success}")

        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке пакета: {e}")
            continue

    duration = time.time() - start_time
    logger.info(f"🏁 ИТОГО: {success}/{total} документов за {duration:.1f}с. Ошибок: {len(errors)}")

    if errors:
        logger.warning(f"⚠️ Примеры ошибок: {errors[:3]}")

    return success