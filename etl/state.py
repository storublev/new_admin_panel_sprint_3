"""Хранение состояния ETL-процесса.

Состояние хранится в JSON-файле на диске.
При перезапуске приложение продолжает с последней обработанной даты.
"""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class JsonFileStorage:
    """Хранилище состояния в виде JSON-файла."""

    def __init__(self, file_path: str) -> None:
        self._file_path = file_path

    def save(self, state: dict[str, Any]) -> None:
        """Сохраняет словарь состояния в JSON-файл."""
        try:
            with open(self._file_path, "w", encoding="utf-8") as f:
                json.dump(state, f)
        except OSError as exc:
            logger.error("Не удалось сохранить состояние: %s", exc)

    def load(self) -> dict[str, Any]:
        """Загружает состояние из JSON-файла.

        Returns:
            Словарь состояния. Если файл не существует или повреждён —
            возвращается пустой словарь.
        """
        try:
            with open(self._file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}


class State:
    """Обёртка над хранилищем для удобного чтения/записи ключей."""

    def __init__(self, storage: JsonFileStorage) -> None:
        self._storage = storage
        self._data = self._storage.load()

    def get(self, key: str, default: Any = None) -> Any:
        """Возвращает значение по ключу или default."""
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Устанавливает значение по ключу и сохраняет."""

        self._data[key] = value
        self._storage.save(self._data)