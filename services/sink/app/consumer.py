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
from prometheus_client import Counter, start_http_server

from .settings import settings
from .transform import to_record, parse_topic, storage_location, TransformError
from .writer import DeltaWriter

ROWS_WRITTEN = Counter("ztap_sink_rows_written_total", "CDC rows written to Delta", ["table"])
FLUSHES = Counter("ztap_sink_flushes_total", "Number of Delta flush commits")

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


def _app_id(topic: str, partition: int) -> str:
    return f"{topic}::{partition}"


def run() -> None:
    import json
    from deltalake import DeltaTable
    from confluent_kafka import TopicPartition

    so = settings.storage_options()
    writer = DeltaWriter(so)
    start_http_server(8000)  # expose /metrics for Prometheus
    log.info("sink metrics on :8000/metrics")

    # Exactly-once recovery: on partition assignment, resume each partition from
    # the offset Delta already durably committed (read from the Delta table's app
    # transaction), not just from Kafka's committed offset. This means a lost
    # offset commit can never cause a re-write.
    def on_assign(c, partitions):
        for tp in partitions:
            try:
                project, table = parse_topic(tp.topic)
                uri = storage_location(project, table, settings.bucket)
                txns = DeltaTable(uri, storage_options=so).transaction_versions()
                last = txns.get(_app_id(tp.topic, tp.partition))
                if last is not None:
                    tp.offset = last + 1
                    log.info("resuming %s[%d] from Delta offset %d", tp.topic, tp.partition, last + 1)
            except Exception:  # noqa: BLE001
                pass  # table doesn't exist yet -> default (earliest/committed)
        c.assign(partitions)

    consumer = build_consumer()
    consumer.subscribe([settings.topic_pattern], on_assign=on_assign)
    log.info("sink started: pattern=%s bootstrap=%s", settings.topic_pattern, settings.kafka_bootstrap)

    batch: dict[str, list[dict]] = defaultdict(list)
    # per table_uri: {(topic, partition): max_offset} for the app-transaction marker
    offsets: dict[str, dict[tuple[str, int], int]] = defaultdict(dict)
    count = 0
    last_flush = time.monotonic()

    def flush():
        nonlocal batch, offsets, count, last_flush
        if not batch:
            last_flush = time.monotonic()
            return
        for table_uri, rows in batch.items():
            app_txns = [(_app_id(t, p), off) for (t, p), off in offsets[table_uri].items()]
            try:
                written = writer.append(table_uri, rows, app_transactions=app_txns)
            except Exception:  # noqa: BLE001
                log.exception("failed writing %d rows to %s", len(rows), table_uri)
                raise
            if written:
                ROWS_WRITTEN.labels(table=table_uri.rsplit("/", 2)[-1]).inc(written)
        FLUSHES.inc()
        consumer.commit(asynchronous=False)
        log.info("flushed %d records across %d tables", count, len(batch))
        batch = defaultdict(list)
        offsets = defaultdict(dict)
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
            key = (msg.topic(), msg.partition())
            prev = offsets[uri].get(key, -1)
            if msg.offset() > prev:
                offsets[uri][key] = msg.offset()
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
