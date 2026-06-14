#!/bin/bash
# Ждём, пока PostgreSQL и Elasticsearch будут готовы.

DB_HOST="${DB_HOST:-db}"
DB_PORT="${DB_PORT:-5432}"
ES_HOST="${ELASTIC_HOST:-http://elastic:9200}"

echo "Ожидаем PostgreSQL ($DB_HOST:$DB_PORT)…"
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U app 2>/dev/null; do
  sleep 2
done
echo "PostgreSQL готов."

echo "Ожидаем Elasticsearch ($ES_HOST)…"
until curl -s "$ES_HOST" >/dev/null 2>&1; do
  sleep 2
done
echo "Elasticsearch готов. Запускаем ETL."

exec python -u /opt/etl/etl.py