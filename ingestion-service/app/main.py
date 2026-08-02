from __future__ import annotations

import json
import logging
import os
import time

from kafka import KafkaProducer

from app.normalizer import normalize_transaction
from app.sources import BitcoinCoreSource, SimulatedSource, TransactionSource

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("ingestion-service")


def build_source() -> TransactionSource:
    source_type = os.getenv("SOURCE_TYPE", "simulated").strip().lower()
    if source_type == "simulated":
        return SimulatedSource()
    if source_type == "bitcoin_core":
        return BitcoinCoreSource()
    raise ValueError(f"Unsupported SOURCE_TYPE={source_type}")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        linger_ms=int(os.getenv("KAFKA_LINGER_MS", "50")),
        retries=10,
    )


def main() -> None:
    source = build_source()
    producer = build_producer()
    topic = os.getenv("TRANSACTIONS_TOPIC", "btc.transactions")
    emitted = 0

    for raw_event in source.stream():
        normalized = normalize_transaction(raw_event).to_dict()
        producer.send(topic, key=normalized["tx_id"], value=normalized)
        emitted += 1

        if emitted % 250 == 0:
            producer.flush()
            logger.info("published=%s source=%s topic=%s", emitted, normalized["source"], topic)
        else:
            time.sleep(0)


if __name__ == "__main__":
    main()
