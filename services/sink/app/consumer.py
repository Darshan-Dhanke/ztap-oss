"""Kafka -> Delta sink consumer loop.

Subscribes to every ztap CDC topic, batches records per target table, and
appends them to the matching Delta table in MinIO. Offsets are committed only
after a successful write, so a crash re-delivers rather than loses data
(at-least-once).
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError, KafkaException

from .settings import settings
from .transform import to_record, storage_location, TransformError
from .writer import DeltaWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("ztap.sink")


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": settings.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        # Pattern subscriptions only pick up new topics on metadata refresh;
        # the default is 5 minutes, far too slow for "create project -> see
        # data". Refresh every 5s so a new project's topic is consumed promptly.
        "topic.metadata.refresh.interval.ms": 5000,
        "allow.auto.create.topics": False,
    })


def run() -> None:
    import json

    consumer = build_consumer()
    consumer.subscribe([settings.topic_pattern])
    writer = DeltaWriter(settings.storage_options())
    log.info("sink started: pattern=%s bootstrap=%s", settings.topic_pattern, settings.kafka_bootstrap)

    batch: dict[str, list[dict]] = defaultdict(list)
    count = 0
    last_flush = time.monotonic()

    def flush():
        nonlocal batch, count, last_flush
        if not batch:
            last_flush = time.monotonic()
            return
        for table_uri, rows in batch.items():
            try:
                writer.append(table_uri, rows)
            except Exception:  # noqa: BLE001
                log.exception("failed writing %d rows to %s", len(rows), table_uri)
                raise
        consumer.commit(asynchronous=False)
        log.info("flushed %d records across %d tables", count, len(batch))
        batch = defaultdict(list)
        count = 0
        last_flush = time.monotonic()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.monotonic() - last_flush >= settings.batch_max_seconds:
                    flush()
                continue
            if msg.error():
                err = msg.error()
                # These are informational, not failures: end-of-partition, and
                # "topic not available" which is normal for a pattern
                # subscription before any matching topic exists.
                if err.code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                if err.retriable():
                    log.warning("retriable kafka error: %s", err)
                    continue
                raise KafkaException(err)

            raw = msg.value()
            value = json.loads(raw) if raw else None
            try:
                record = to_record(msg.topic(), value)
            except TransformError as e:
                log.warning("skipping message: %s", e)
                continue
            if record is None:
                continue

            uri = storage_location(record.project, record.table, settings.bucket)
            batch[uri].append(record.row)
            count += 1

            if count >= settings.batch_max_records or \
                    time.monotonic() - last_flush >= settings.batch_max_seconds:
                flush()
    except KeyboardInterrupt:
        log.info("shutting down")
    finally:
        try:
            flush()
        finally:
            consumer.close()


if __name__ == "__main__":
    run()
