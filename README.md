# ETL

---

## Описание структуры:

Проект представляет собой реализацию ETL, в процессе которого данные о фильмах, хранящиеся в
PostgreSQL переносятся в БД Elasticsearch.

Процесс реализован с применением [Docker](https://www.docker.com/)-контейнеров:

1. [Elasticsearch](https://www.elastic.co) — NoSQL БД для полнотекстового поиска;
2. [PostgreSQL](https://www.postgresql.org/) — SQL база данных (БД);
3. [Redis](https://redis.io) — key-value БД (в проекте используется для хранения состояния переноса данных)
4. [Kibana](https://www.elastic.co/kibana/) - веб-интерфейс для взаимодействия с Elasticsearch
5. ETL-скрипт, реализованный на языке Python

Сервисы связаны между собой с помощью `docker compose` и зависят друг от друга.
Для упрощения работы в проекте предусмотрен Makefile.


##  Порядок запуска проекта:

Для быстрого и полного запуска проекта достаточно, находясь в корне репозитория,
выполнить команду `make full-up`.

Данная команда является составной и выполняет следующее:
1. создаст контейнеры на основе `docker-compose.yml` файла,
а также именованные `volume`, предназначенные для хранения файлов, данных из БД и т.п..
2. запустит ETL-script для переноса данных в хранилище Elasticsearch


Каждое из описанных выше в списке действий может быть выполнено с помощью
отдельных команд:
1. `make dev-up`
2. `make load-data` 


Таким образом, можно запустить "_голый_" проект и выполнить только те команды,
которые требуется.

Больше команд прописано в `Makefile` 

## Требования для запуска проекта:
1. установленный и запущенный Docker Daemon
2. доступ к интернету
_3. не менее 1 Гб свободного пространства на жестком диске_


Данные запрашиваются из PostgreSQL и загружаются в Elasticsearch пачками размером не более 100 записей.
В процессе переноса данных отслеживаются изменения в PostgreSQL, и вносимые изменения переносятся 
в Elasticsearch. 
В качестве хранимого состояния используется отдельная таблица audit_log

Пример для тестового добавления фильма:
-- Добавляем тестовый фильм
INSERT INTO content.film_work (
    id, 
    title, 
    description, 
    rating, 
    type, 
    modified, 
    creation_date
)
VALUES (
    gen_random_uuid(),
    'Тестовый фильм ETL ' || NOW(),
    'Это тестовый фильм для проверки ETL. Создан в ' || NOW(),
    (random() * 4 + 5)::numeric(3,1),  -- Рейтинг от 5 до 9
    'movie',
    NOW(),
    NOW()
);
-- Проверяем, что триггер сработал
SELECT 
    al.id,
    al.record_id,
    al.operation,
    al.processed,
    al.changed_at,
    fw.title
FROM content.audit_log al
JOIN content.film_work fw ON fw.id::VARCHAR = al.record_id
WHERE al.table_name = 'film_work'
AND al.operation = 'I'
ORDER BY al.id DESC
LIMIT 5;

## 1. Проверить, что фильм добавился в БД
docker-compose exec db psql -U app -d movies_database -c "
SELECT id, title, type, rating FROM content.film_work 
WHERE title LIKE '%Тестовый%' 
ORDER BY created_at DESC 
LIMIT 5;
"

## 2. Проверить audit_log
docker-compose exec db psql -U app -d movies_database -c "
SELECT id, record_id, operation, processed, changed_at 
FROM content.audit_log 
WHERE table_name = 'film_work' 
ORDER BY id DESC 
LIMIT 5;
"

## 3. Проверить Elasticsearch
curl -s "http://localhost:9200/movies/_search?q=*&size=5" | python -m json.tool

## 4. Проверить количество документов в Elasticsearch
curl -s "http://localhost:9200/movies/_count" | python -m json.tool
