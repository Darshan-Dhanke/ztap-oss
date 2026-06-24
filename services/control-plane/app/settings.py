import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    pg_host: str = os.getenv("PG_HOST", "localhost")
    pg_port: int = int(os.getenv("PG_PORT", "5432"))
    pg_user: str = os.getenv("PG_USER", "ztap")
    pg_password: str = os.getenv("PG_PASSWORD", "ztap")
    pg_db: str = os.getenv("PG_DB", "ztap")

    connect_url: str = os.getenv("CONNECT_URL", "http://localhost:8083")
    uc_url: str = os.getenv("UC_URL", "http://localhost:8080")
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    proxy_url: str = os.getenv("PROXY_URL", "http://localhost:8002")

    # When true, external infra calls (UC, Debezium) are skipped/mocked. Used by
    # unit tests so the provisioning logic can be exercised without a live stack.
    offline: bool = os.getenv("ZTAP_OFFLINE", "0") == "1"


settings = Settings()
