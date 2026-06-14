"""Конфигурация приложения с валидацией через pydantic."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Настройки подключения к внешним сервисам и параметры ETL."""

    db_name: str = "movies_database"
    db_user: str = "app"
    db_password: str = "123qwe"
    db_host: str = "localhost"
    db_port: int = 5432
    db_schema: str = "content"

    elastic_host: str = "http://localhost:9200"

    batch_size: int = 100
    poll_interval: int = 60
    state_file: str = "/tmp/etl_state.json"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @property
    def dsn(self) -> str:
        """Строка подключения к PostgreSQL."""
        return (
            f"host={self.db_host} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_password} "
            f"port={self.db_port} options='-c search_path={self.db_schema}'"
        )


settings = Settings()