import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
    group_id: str = os.getenv("SINK_GROUP_ID", "ztap-delta-sink")
    topic_pattern: str = os.getenv("SINK_TOPIC_PATTERN", "^ztap\\..*")

    # MinIO / S3
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", "minioadmin")
    s3_region: str = os.getenv("S3_REGION", "us-east-1")
    bucket: str = os.getenv("WAREHOUSE_BUCKET", "warehouse")

    # Batching: flush when either threshold is hit.
    batch_max_records: int = int(os.getenv("SINK_BATCH_MAX_RECORDS", "500"))
    batch_max_seconds: float = float(os.getenv("SINK_BATCH_MAX_SECONDS", "5"))

    def storage_options(self) -> dict:
        return {
            "AWS_ENDPOINT_URL": self.s3_endpoint,
            "AWS_ACCESS_KEY_ID": self.s3_access_key,
            "AWS_SECRET_ACCESS_KEY": self.s3_secret_key,
            "AWS_REGION": self.s3_region,
            "AWS_ALLOW_HTTP": "true",
            # single-writer sink, so the unsafe rename (no DynamoDB lock) is fine
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }


settings = Settings()
