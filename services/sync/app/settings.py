import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    pg_host: str = os.getenv("PG_HOST", "localhost")
    pg_port: int = int(os.getenv("PG_PORT", "5432"))
    pg_user: str = os.getenv("PG_USER", "ztap")
    pg_password: str = os.getenv("PG_PASSWORD", "ztap")
    pg_db: str = os.getenv("PG_DB", "ztap")
    uc_url: str = os.getenv("UC_URL", "http://localhost:8080")
    bucket: str = os.getenv("WAREHOUSE_BUCKET", "warehouse")

    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} user={self.pg_user} "
            f"password={self.pg_password} dbname={self.pg_db}"
        )


settings = Settings()
