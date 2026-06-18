#!/usr/bin/env python
"""ETL-процесс с использованием Change Data Capture (CDC).

Основной функционал:
1. Отслеживание изменений в PostgreSQL через таблицу аудита
2. Синхронизация данных с Elasticsearch
3. Обработка INSERT, UPDATE, DELETE операций
4. Хранение состояния в PostgreSQL
5. Автоматическое восстановление после сбоев
"""

import logging
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.config import settings
from core.queries import FETCH_MOVIE_BY_ID
from db.elastic import get_es_client, ensure_index, bulk_upload_with_progress, bulk_upload
from db.postgres import get_pg_connection, backoff
from models.dataclasses import Movie, Person, Genre
from state import State, DatabaseStateStorage

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, getattr(settings, 'log_level', 'INFO'), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# Константы
STATE_KEY_AUDIT = "last_audit_id"
STATE_KEY_MODIFIED = "last_modified"
MOVIES_INDEX = "movies"


def transform_row_to_movie(row: Dict[str, Any]) -> Movie:
    """Преобразует строку из БД в объект Movie.

    Args:
        row: Словарь с данными фильма из PostgreSQL

    Returns:
        Объект Movie с разделенными персонами по ролям
    """
    actors: List[Person] = []
    directors: List[Person] = []
    writers: List[Person] = []

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


def to_bulk_action(movie: Movie) -> Dict[str, Any]:
    """Формирует bulk-действие для Elasticsearch.

    Args:
        movie: Объект фильма

    Returns:
        Словарь для индексации в Elasticsearch
    """
    return {
        "_index": MOVIES_INDEX,
        "_id": movie.id,
        "_source": movie.to_es_document(),
    }


class AuditETL:
    """ETL с отслеживанием изменений через таблицу аудита."""

    def __init__(self):
        """Инициализация ETL сервиса."""
        # Инициализируем состояние в БД
        self.storage = DatabaseStateStorage()
        self.state = State(self.storage)

        # Последний обработанный ID в таблице аудита
        self.last_audit_id = int(self.state.get(STATE_KEY_AUDIT, 0))

        # Счетчики для статистики
        self.stats = {
            "total_processed": 0,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "errors": 0,
            "start_time": time.time(),
            "last_processed": None,
        }

        # Загружаем сохраненную статистику
        saved_stats = self.state.get_statistics()
        if saved_stats:
            logger.info(f"📊 Загружена статистика: {saved_stats.get('total_processed', 0)} всего обработано")

        # Подключаемся к Elasticsearch
        logger.info("🔌 Подключение к Elasticsearch...")
        self.es = get_es_client()
        ensure_index(self.es)
        logger.info("✅ Elasticsearch готов")

        logger.info(f"📌 ETL инициализирован. Последний audit_id: {self.last_audit_id}")

    @backoff(max_retries=5)
    def get_unprocessed_changes(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Получает необработанные изменения из аудит-лога.

        Args:
            limit: Максимальное количество записей за раз

        Returns:
            Список изменений
        """
        conn = get_pg_connection()
        try:
            with conn.cursor(cursor_factory=conn.cursor_factory) as cursor:
                cursor.execute("""
                    SELECT 
                        id,
                        table_name,
                        record_id,
                        operation,
                        changed_at,
                        old_data,
                        new_data
                    FROM content.audit_log
                    WHERE id > %s
                      AND processed = FALSE
                    ORDER BY id ASC
                    LIMIT %s
                """, (self.last_audit_id, limit))

                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка при получении изменений: {e}")
            raise
        finally:
            conn.close()

    @backoff(max_retries=5)
    def mark_changes_processed(self, change_ids: List[int]) -> None:
        """Отмечает изменения как обработанные.

        Args:
            change_ids: Список ID изменений для отметки
        """
        if not change_ids:
            return

        conn = get_pg_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE content.audit_log
                    SET processed = TRUE,
                        processed_at = CURRENT_TIMESTAMP
                    WHERE id = ANY(%s)
                """, (change_ids,))
                conn.commit()
                logger.debug(f"✅ Отмечено {len(change_ids)} изменений как обработанные")
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении статуса: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    @backoff(max_retries=5)
    def fetch_movie_data(self, movie_id: str) -> Optional[Dict[str, Any]]:
        """Получает полные данные фильма из БД.

        Args:
            movie_id: ID фильма

        Returns:
            Словарь с данными фильма или None если не найден
        """
        conn = get_pg_connection()
        try:
            with conn.cursor(cursor_factory=conn.cursor_factory) as cursor:
                query = FETCH_MOVIE_BY_ID.format(movie_id=movie_id)
                cursor.execute(query)
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"❌ Ошибка при получении фильма {movie_id}: {e}")
            raise
        finally:
            conn.close()

    def process_insert_update(self, change: Dict[str, Any]) -> bool:
        """Обрабатывает INSERT или UPDATE запись.

        Args:
            change: Словарь с изменением

        Returns:
            True если успешно, False если ошибка
        """
        try:
            record_id = change['record_id']

            # Получаем актуальные данные из БД
            movie_data = self.fetch_movie_data(record_id)

            if not movie_data:
                logger.warning(f"⚠️ Фильм {record_id} не найден в БД, возможно удален")
                return False

            # Трансформируем в объект Movie
            movie = transform_row_to_movie(movie_data)
            bulk_action = to_bulk_action(movie)

            # Загружаем в Elasticsearch
            result = self.es.index(
                index=MOVIES_INDEX,
                id=movie.id,
                body=bulk_action["_source"],
                refresh=True,  # Для тестов можно включить
            )

            if result.get('result') in ['created', 'updated']:
                logger.debug(f"✅ Индексирован фильм: {movie.id} - {movie.title}")
                return True
            else:
                logger.error(f"❌ Ошибка индексации: {movie.id}, результат: {result}")
                return False

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке INSERT/UPDATE {change.get('record_id')}: {e}")
            self.stats["errors"] += 1
            return False

    def process_delete(self, change: Dict[str, Any]) -> bool:
        """Обрабатывает DELETE запись.

        Args:
            change: Словарь с изменением

        Returns:
            True если успешно, False если ошибка
        """
        try:
            record_id = change['record_id']

            # Удаляем из Elasticsearch
            result = self.es.delete(
                index=MOVIES_INDEX,
                id=record_id,
                ignore=[404],
                refresh=True  # Для тестов можно включить
            )

            if result.get('result') in ['deleted', 'not_found']:
                logger.debug(f"🗑️ Удален фильм: {record_id}")
                return True
            else:
                logger.warning(f"⚠️ Не удалось удалить: {record_id}, результат: {result}")
                return False

        except Exception as e:
            logger.error(f"❌ Ошибка при удалении {change.get('record_id')}: {e}")
            self.stats["errors"] += 1
            return False

    def process_changes(self, changes: List[Dict[str, Any]]) -> int:
        """Обрабатывает список изменений с батчевой загрузкой."""
        if not changes:
            return 0

        logger.info(f"📦 Обработка {len(changes)} изменений...")

        # Группируем по операции
        grouped = {'I': [], 'U': [], 'D': []}
        for change in changes:
            op = change['operation']
            if op in grouped:
                grouped[op].append(change)

        processed_count = 0
        change_ids = []
        max_audit_id = self.last_audit_id

        # Собираем документы для батчевой загрузки (INSERT и UPDATE)
        bulk_actions = []
        processed_changes = []

        # Обрабатываем INSERT и UPDATE через bulk
        for op in ['I', 'U']:
            if not grouped.get(op):
                continue

            op_name = {'I': 'INSERT', 'U': 'UPDATE'}[op]
            logger.info(f"🔄 Обработка {op_name}: {len(grouped[op])} записей")

            for change in grouped[op]:
                record_id = change['record_id']

                # Получаем данные фильма
                movie_data = self.fetch_movie_data(record_id)

                if not movie_data:
                    logger.warning(f"⚠️ Фильм {record_id} не найден")
                    continue

                movie = transform_row_to_movie(movie_data)
                bulk_action = to_bulk_action(movie)
                bulk_actions.append(bulk_action)
                processed_changes.append(change)

                if change['id'] > max_audit_id:
                    max_audit_id = change['id']

        # Загружаем батчем по 100 документов
        if bulk_actions:
            logger.info(f"📤 Загрузка {len(bulk_actions)} документов в Elasticsearch...")

            # Разбиваем на батчи по 100
            batch_size = 100
            total_success = 0

            for i in range(0, len(bulk_actions), batch_size):
                batch = bulk_actions[i:i + batch_size]
                batch_changes = processed_changes[i:i + batch_size]

                success_count = bulk_upload_with_progress(
                    self.es,
                    batch,
                    batch_size=100
                )

                total_success += success_count

                # Отмечаем успешно загруженные изменения
                if success_count > 0:
                    for j in range(min(success_count, len(batch_changes))):
                        change_ids.append(batch_changes[j]['id'])
                        self.stats['total_processed'] += 1
                        self.stats['inserts'] += 1

        # Обрабатываем DELETE отдельно
        if grouped.get('D'):
            logger.info(f"🔄 Обработка DELETE: {len(grouped['D'])} записей")
            for change in grouped['D']:
                success = self.process_delete(change)
                if success:
                    processed_count += 1
                    self.stats['deletes'] += 1
                    change_ids.append(change['id'])
                    if change['id'] > max_audit_id:
                        max_audit_id = change['id']

        # Отмечаем изменения как обработанные
        if change_ids:
            self.mark_changes_processed(change_ids)
            self.last_audit_id = max_audit_id
            self.state.set(STATE_KEY_AUDIT, str(self.last_audit_id))

            self.state.increment_counter('total_processed', len(change_ids))
            self.state.increment_counter('total_runs', 1)

            self.state.update_statistics({
                'last_run': datetime.now().isoformat(),
                'last_audit_id': self.last_audit_id,
                'inserts': self.stats['inserts'],
                'updates': self.stats['updates'],
                'deletes': self.stats['deletes'],
                'errors': self.stats['errors'],
            })

            logger.info(f"💾 Состояние сохранено в БД: audit_id={self.last_audit_id}")

        return len(change_ids)

    def check_elasticsearch_data(self) -> int:
        """Проверяет, есть ли данные в Elasticsearch."""
        try:
            result = self.es.count(index=MOVIES_INDEX)
            count = result.get('count', 0)
            logger.info(f"📊 В Elasticsearch найдено {count} документов")
            return count
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке данных в Elasticsearch: {e}")
            return 0

    def initial_load(self) -> int:
        """Выполняет начальную загрузку всех фильмов из PostgreSQL в Elasticsearch."""
        logger.info("📦 Начальная загрузка всех фильмов...")

        conn = get_pg_connection()
        try:
            with conn.cursor(cursor_factory=conn.cursor_factory) as cursor:
                # Получаем все фильмы с пагинацией
                offset = 0
                batch_size = 100
                total_loaded = 0

                while True:
                    # Запрос с пагинацией
                    cursor.execute("""
                        SELECT fw.id,
                               fw.rating AS imdb_rating,
                               fw.title,
                               fw.description,
                               fw.modified,
                               fw.creation_date,
                               COALESCE(
                                   json_agg(
                                       DISTINCT jsonb_build_object('id', g.id, 'name', g.name)
                                   ) FILTER (WHERE g.id IS NOT NULL),
                                   '[]'::json
                               ) AS genres,
                               COALESCE(
                                   json_agg(
                                       DISTINCT jsonb_build_object(
                                           'id', p.id,
                                           'name', p.full_name,
                                           'role', pfw.role
                                       )
                                   ) FILTER (WHERE p.id IS NOT NULL),
                                   '[]'::json
                               ) AS persons
                        FROM content.film_work fw
                        LEFT JOIN content.genre_film_work  gfw  ON gfw.film_work_id = fw.id
                        LEFT JOIN content.genre            g    ON g.id = gfw.genre_id
                        LEFT JOIN content.person_film_work pfw  ON pfw.film_work_id = fw.id
                        LEFT JOIN content.person           p    ON p.id = pfw.person_id
                        GROUP BY fw.id, fw.rating, fw.title, fw.description, fw.modified, fw.creation_date
                        ORDER BY fw.modified, fw.id
                        LIMIT %s OFFSET %s
                    """, (batch_size, offset))

                    movies = [dict(row) for row in cursor.fetchall()]

                    if not movies:
                        break

                    logger.info(f"📦 Загрузка пакета {offset // batch_size + 1}: {len(movies)} фильмов")

                    # Трансформируем и загружаем
                    transformed = [transform_row_to_movie(row) for row in movies]
                    bulk_actions = [to_bulk_action(m) for m in transformed]

                    success = bulk_upload_with_progress(self.es, bulk_actions, batch_size=100)
                    total_loaded += success

                    # Обновляем состояние для каждого пакета
                    if movies:
                        last_modified = movies[-1]['modified']
                        if hasattr(last_modified, 'isoformat'):
                            last_modified = last_modified.isoformat()
                        else:
                            last_modified = str(last_modified)
                        self.state.set(STATE_KEY_MODIFIED, last_modified)

                    offset += batch_size

                    # Небольшая пауза между пакетами
                    time.sleep(0.1)

                logger.info(f"✅ Начальная загрузка завершена: {total_loaded} фильмов")
                return total_loaded

        except Exception as e:
            logger.error(f"❌ Ошибка при начальной загрузке: {e}")
            raise
        finally:
            conn.close()

    def ensure_elasticsearch_data(self):
        """Проверяет и при необходимости загружает данные в Elasticsearch."""
        # Проверяем, есть ли данные в Elasticsearch
        count = self.check_elasticsearch_data()

        if count > 0:
            logger.info(f"✅ В Elasticsearch уже есть {count} документов, начальная загрузка не требуется")
            return

        logger.info("📭 Elasticsearch пуст, выполняю начальную загрузку...")

        # Проверяем, есть ли данные в audit_log
        conn = get_pg_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM content.audit_log WHERE processed = TRUE")
                processed_count = cursor.fetchone()[0]

                if processed_count > 0:
                    logger.info(f"📊 В audit_log уже есть {processed_count} обработанных записей")
                    # Проверяем, сколько всего фильмов в БД
                    cursor.execute("SELECT COUNT(*) FROM content.film_work")
                    total_films = cursor.fetchone()[0]

                    if processed_count < total_films:
                        logger.info(f"📝 Добавляем недостающие записи в audit_log...")
                        cursor.execute("""
                            INSERT INTO content.audit_log (table_name, record_id, operation, new_data, processed)
                            SELECT 
                                'film_work'::VARCHAR,
                                fw.id::VARCHAR,
                                'I'::CHAR,
                                to_jsonb(fw.*),
                                FALSE
                            FROM content.film_work fw
                            WHERE NOT EXISTS (
                                SELECT 1 FROM content.audit_log al 
                                WHERE al.record_id = fw.id::VARCHAR
                                AND al.table_name = 'film_work'
                            )
                        """)
                        conn.commit()
                        logger.info(f"✅ Добавлено {cursor.rowcount} записей в audit_log")

        finally:
            conn.close()

        # Выполняем начальную загрузку
        self.initial_load()

    def print_statistics(self):
        """Выводит статистику работы ETL."""
        stats = self.state.get_statistics()
        runtime = time.time() - self.stats['start_time']

        logger.info("=" * 60)
        logger.info("📊 СТАТИСТИКА ETL")
        logger.info(f"   Всего обработано: {stats.get('total_processed', 0)}")
        logger.info(f"   Запусков: {stats.get('total_runs', 0)}")
        logger.info(f"   INSERT: {self.stats['inserts']}")
        logger.info(f"   UPDATE: {self.stats['updates']}")
        logger.info(f"   DELETE: {self.stats['deletes']}")
        logger.info(f"   Ошибок: {self.stats['errors']}")
        logger.info(f"   Время работы: {runtime:.2f}с")
        logger.info(f"   Последний audit_id: {self.last_audit_id}")
        if stats.get('last_run'):
            logger.info(f"   Последний запуск: {stats['last_run']}")
        logger.info("=" * 60)

    def run_once(self) -> int:
        """Выполняет один цикл ETL.

        Returns:
            Количество обработанных изменений
        """
        # Получаем новые изменения
        changes = self.get_unprocessed_changes()

        if not changes:
            return 0

        logger.info(f"🔍 Найдено {len(changes)} необработанных изменений")
        processed = self.process_changes(changes)

        if processed > 0:
            logger.info(f"✅ Обработано {processed} изменений")
        else:
            logger.warning("⚠️ Не удалось обработать изменения")

        return processed

    def run(self):
        """Запускает ETL в бесконечном цикле."""
        logger.info("🚀 Запуск ETL с Change Data Capture...")
        logger.info(f"📌 Последний audit_id: {self.last_audit_id}")

        # Проверяем и загружаем данные в Elasticsearch при первом старте
        self.ensure_elasticsearch_data()

        # Получаем актуальный last_audit_id после начальной загрузки
        self.last_audit_id = int(self.state.get(STATE_KEY_AUDIT, 0))
        logger.info(f"📌 Обновленный audit_id: {self.last_audit_id}")

        empty_cycles = 0
        poll_interval = getattr(settings, 'poll_interval', 10)
        sleep_time = getattr(settings, 'sleep_time', 1)

        while True:
            try:
                processed = self.run_once()

                if processed > 0:
                    empty_cycles = 0
                    time.sleep(sleep_time)
                else:
                    empty_cycles += 1

                    if empty_cycles > 10:
                        wait_time = min(60, empty_cycles * 2)
                        logger.debug(f"⏳ Долгое ожидание: {wait_time}с (пустых циклов: {empty_cycles})")
                        time.sleep(wait_time)
                    else:
                        time.sleep(poll_interval)

                if empty_cycles % 10 == 0 and empty_cycles > 0:
                    self.print_statistics()

            except KeyboardInterrupt:
                logger.info("🛑 ETL остановлен пользователем")
                break
            except Exception as e:
                logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
                logger.info(f"⏳ Повторная попытка через {poll_interval}с...")
                time.sleep(poll_interval)


def main():
    """Точка входа."""
    try:
        etl = AuditETL()
        etl.run()
    except Exception as e:
        logger.error(f"❌ Фатальная ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()