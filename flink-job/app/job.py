from __future__ import annotations

import json
import os
import time
from collections import deque
from dataclasses import dataclass

import clickhouse_connect
import redis
from pyflink.common import Configuration, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import KeyedProcessFunction, MapFunction, RuntimeContext, SinkFunction
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.datastream.watermark_strategy import TimestampAssigner, WatermarkStrategy


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "btc.transactions")
ALERTS_TOPIC = os.getenv("ALERTS_TOPIC", "btc.alerts")
METRICS_TOPIC = os.getenv("METRICS_TOPIC", "btc.metrics")
LATE_EVENT_TOLERANCE_MS = int(os.getenv("FLINK_LATE_TOLERANCE_MS", "30000"))


def utc_now_ms() -> int:
    return int(time.time() * 1000)


class JsonParser(MapFunction):
    def map(self, value: str) -> dict:
        return json.loads(value)


class TxTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: dict, _record_timestamp: int) -> int:
        return int(value["timestamp"])


class TransactionMetricMapper(MapFunction):
    def map(self, tx: dict) -> str:
        total_input = round(sum(item["value"] for item in tx["inputs"]), 8)
        total_output = round(sum(item["value"] for item in tx["outputs"]), 8)
        payload = {
            "metric_type": "transaction",
            "tx_id": tx["tx_id"],
            "timestamp": tx["timestamp"],
            "ingest_time": tx.get("ingest_time", utc_now_ms()),
            "source": tx["source"],
            "input_count": len(tx["inputs"]),
            "output_count": len(tx["outputs"]),
            "total_input": total_input,
            "total_output": total_output,
            "fee_estimate": round(max(total_input - total_output, 0), 8),
            "is_late": tx["timestamp"] < tx.get("_watermark", -1),
        }
        return json.dumps(payload)


class TransactionAlertMapper(MapFunction):
    def map(self, tx: dict) -> list[str]:
        alerts: list[str] = []
        total_output = round(sum(item["value"] for item in tx["outputs"]), 8)
        outputs = tx["outputs"]
        inputs = tx["inputs"]

        if len(outputs) >= 8:
            alerts.append(
                json.dumps(
                    {
                        "alert_id": f"{tx['tx_id']}:fanout",
                        "alert_type": "fan_out",
                        "severity": "medium",
                        "timestamp": tx["timestamp"],
                        "tx_id": tx["tx_id"],
                        "address": inputs[0]["address"] if inputs else "coinbase",
                        "details": {"output_count": len(outputs), "total_output": total_output},
                    }
                )
            )

        whale_threshold = float(os.getenv("AML_WHALE_THRESHOLD", "40"))
        if total_output >= whale_threshold:
            alerts.append(
                json.dumps(
                    {
                        "alert_id": f"{tx['tx_id']}:whale",
                        "alert_type": "whale_transaction",
                        "severity": "high",
                        "timestamp": tx["timestamp"],
                        "tx_id": tx["tx_id"],
                        "address": outputs[0]["address"] if outputs else "unknown",
                        "details": {"total_output": total_output},
                    }
                )
            )

        return alerts


class AddressUpdateExtractor(MapFunction):
    def map(self, tx: dict) -> list[dict]:
        updates: list[dict] = []
        watermark = tx.get("_watermark", -1)
        for item in tx["inputs"]:
            updates.append(
                {
                    "kind": "input",
                    "address": item["address"],
                    "tx_id": tx["tx_id"],
                    "timestamp": tx["timestamp"],
                    "value": item["value"],
                    "prev_tx_id": item["prev_tx_id"],
                    "prev_output_index": item["prev_output_index"],
                    "source": tx["source"],
                    "watermark": watermark,
                }
            )
        for item in tx["outputs"]:
            updates.append(
                {
                    "kind": "output",
                    "address": item["address"],
                    "tx_id": tx["tx_id"],
                    "timestamp": tx["timestamp"],
                    "value": item["value"],
                    "output_index": item["output_index"],
                    "source": tx["source"],
                    "watermark": watermark,
                }
            )
        return updates


@dataclass
class BalanceRecord:
    address: str
    balance: float
    utxo_count: int
    event_timestamp: int
    source: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "metric_type": "balance_update",
                "address": self.address,
                "balance": round(self.balance, 8),
                "utxo_count": self.utxo_count,
                "timestamp": self.event_timestamp,
                "source": self.source,
            }
        )


class MetricAndWatermarkEnricher(KeyedProcessFunction):
    def process_element(self, value: dict, ctx: "KeyedProcessFunction.Context"):
        value["_watermark"] = ctx.timer_service().current_watermark()
        yield value


class AddressStateProcessor(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        self.balance_state = runtime_context.get_state(ValueStateDescriptor("balance", float))
        self.utxo_state = runtime_context.get_list_state(ListStateDescriptor("utxos", str))
        self.activity_state = runtime_context.get_list_state(ListStateDescriptor("recent_activity", str))
        self.pending_spend_state = runtime_context.get_list_state(
            ListStateDescriptor("pending_spends", str)
        )
        self.structuring_window_ms = int(os.getenv("AML_STRUCTURING_WINDOW_MS", "120000"))
        self.velocity_window_ms = int(os.getenv("AML_VELOCITY_WINDOW_MS", "60000"))
        self.velocity_tx_threshold = int(os.getenv("AML_VELOCITY_TX_THRESHOLD", "12"))
        self.structuring_threshold = int(os.getenv("AML_STRUCTURING_COUNT_THRESHOLD", "6"))
        self.structuring_ceiling = float(os.getenv("AML_STRUCTURING_CEILING", "1.0"))

    def process_element(self, update: dict, _ctx: "KeyedProcessFunction.Context"):
        address = update["address"]
        current_balance = self.balance_state.value()
        balance = float(current_balance) if current_balance is not None else 0.0
        utxos = {}
        for raw in self.utxo_state.get():
            item = json.loads(raw)
            utxos[(item["tx_id"], item["output_index"])] = item
        pending_spends = set(tuple(json.loads(item)) for item in self.pending_spend_state.get())

        alerts: list[str] = []
        tx_time = int(update["timestamp"])
        watermark = int(update.get("watermark", -1))

        if update["kind"] == "input":
            key = (update["prev_tx_id"], update["prev_output_index"])
            item = utxos.pop(key, None)
            if item is not None:
                balance -= float(item["value"])
            else:
                pending_spends.add(key)
        else:
            key = (update["tx_id"], update["output_index"])
            if key in pending_spends:
                pending_spends.remove(key)
            else:
                utxo = {
                    "tx_id": update["tx_id"],
                    "output_index": update["output_index"],
                    "value": round(float(update["value"]), 8),
                    "timestamp": tx_time,
                }
                utxos[key] = utxo
                balance += float(update["value"])

        history = deque(json.loads(item) for item in self.activity_state.get())
        history.append(
            {
                "timestamp": tx_time,
                "kind": update["kind"],
                "value": round(float(update["value"]), 8),
            }
        )

        while history and tx_time - history[0]["timestamp"] > max(
            self.structuring_window_ms, self.velocity_window_ms
        ):
            history.popleft()

        recent_velocity = [item for item in history if tx_time - item["timestamp"] <= self.velocity_window_ms]
        if len(recent_velocity) >= self.velocity_tx_threshold:
            alerts.append(
                json.dumps(
                    {
                        "alert_id": f"{update['tx_id']}:{address}:velocity",
                        "alert_type": "velocity_spike",
                        "severity": "medium",
                        "timestamp": tx_time,
                        "tx_id": update["tx_id"],
                        "address": address,
                        "details": {
                            "events_in_window": len(recent_velocity),
                            "window_ms": self.velocity_window_ms,
                        },
                    }
                )
            )

        recent_structured = [
            item
            for item in history
            if item["kind"] == "output"
            and item["value"] <= self.structuring_ceiling
            and tx_time - item["timestamp"] <= self.structuring_window_ms
        ]
        if len(recent_structured) >= self.structuring_threshold:
            alerts.append(
                json.dumps(
                    {
                        "alert_id": f"{update['tx_id']}:{address}:structuring",
                        "alert_type": "structuring",
                        "severity": "high",
                        "timestamp": tx_time,
                        "tx_id": update["tx_id"],
                        "address": address,
                        "details": {
                            "events_in_window": len(recent_structured),
                            "window_ms": self.structuring_window_ms,
                            "ceiling": self.structuring_ceiling,
                        },
                    }
                )
            )

        if tx_time < watermark:
            alerts.append(
                json.dumps(
                    {
                        "alert_id": f"{update['tx_id']}:{address}:late",
                        "alert_type": "late_event",
                        "severity": "low",
                        "timestamp": tx_time,
                        "tx_id": update["tx_id"],
                        "address": address,
                        "details": {"watermark": watermark, "lateness_ms": watermark - tx_time},
                    }
                )
            )

        self.balance_state.update(round(balance, 8))
        self.utxo_state.update(
            [json.dumps(item) for item in sorted(utxos.values(), key=lambda current: (current["tx_id"], current["output_index"]))]
        )
        self.activity_state.update([json.dumps(item) for item in history])
        self.pending_spend_state.update([json.dumps(list(item)) for item in sorted(pending_spends)])

        record = BalanceRecord(
            address=address,
            balance=round(balance, 8),
            utxo_count=len(utxos),
            event_timestamp=tx_time,
            source=update["source"],
        )
        yield {"kind": "balance", "payload": record.to_json()}

        for alert in alerts:
            yield {"kind": "alert", "payload": alert}


class RedisBalanceSink(SinkFunction):
    def open(self, _runtime_context: RuntimeContext) -> None:
        self.client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def invoke(self, value: str, _context) -> None:
        payload = json.loads(value)
        self.client.hset(
            "balances",
            payload["address"],
            json.dumps(
                {
                    "balance": payload["balance"],
                    "utxo_count": payload["utxo_count"],
                    "timestamp": payload["timestamp"],
                    "source": payload["source"],
                }
            ),
        )


class ClickHouseJsonSink(SinkFunction):
    def __init__(self, table: str) -> None:
        self.table = table

    def open(self, _runtime_context: RuntimeContext) -> None:
        self.client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

    def invoke(self, value, _context) -> None:
        payload = json.loads(value) if isinstance(value, str) else value
        if self.table == "transactions":
            self.client.insert(
                "btc_analytics.transactions",
                [[
                    payload["tx_id"],
                    payload["timestamp"],
                    payload.get("ingest_time", utc_now_ms()),
                    payload["source"],
                    len(payload["inputs"]),
                    len(payload["outputs"]),
                    sum(item["value"] for item in payload["inputs"]),
                    sum(item["value"] for item in payload["outputs"]),
                    json.dumps(payload["inputs"]),
                    json.dumps(payload["outputs"]),
                    1 if payload["timestamp"] < payload.get("_watermark", -1) else 0,
                ]],
                column_names=[
                    "tx_id",
                    "timestamp_ms",
                    "ingest_time_ms",
                    "source",
                    "input_count",
                    "output_count",
                    "total_input",
                    "total_output",
                    "inputs_json",
                    "outputs_json",
                    "is_late",
                ],
            )
        elif self.table == "alerts":
            self.client.insert(
                "btc_analytics.alerts",
                [[
                    payload["alert_id"],
                    payload["alert_type"],
                    payload["severity"],
                    payload["timestamp"],
                    payload["tx_id"],
                    payload["address"],
                    json.dumps(payload["details"]),
                ]],
                column_names=[
                    "alert_id",
                    "alert_type",
                    "severity",
                    "timestamp_ms",
                    "tx_id",
                    "address",
                    "details_json",
                ],
            )
        elif self.table == "balance_updates":
            self.client.insert(
                "btc_analytics.balance_updates",
                [[
                    payload["address"],
                    payload["timestamp"],
                    payload["source"],
                    payload["balance"],
                    payload["utxo_count"],
                ]],
                column_names=["address", "timestamp_ms", "source", "balance", "utxo_count"],
            )
        elif self.table == "metrics":
            self.client.insert(
                "btc_analytics.metrics",
                [[
                    payload["metric_type"],
                    payload["timestamp"],
                    payload.get("source", "unknown"),
                    payload.get("tx_id", ""),
                    payload.get("address", ""),
                    json.dumps(payload),
                ]],
                column_names=[
                    "metric_type",
                    "timestamp_ms",
                    "source",
                    "tx_id",
                    "address",
                    "payload_json",
                ],
            )


def build_env() -> StreamExecutionEnvironment:
    config = Configuration()
    config.set_string("pipeline.name", "bitcoin-like-streaming-analytics")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "2")))
    env.enable_checkpointing(int(os.getenv("FLINK_CHECKPOINT_MS", "10000")))
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_min_pause_between_checkpoints(5000)
    checkpoint_config.set_checkpoint_timeout(120000)
    checkpoint_config.set_tolerable_checkpoint_failure_number(3)
    return env


def kafka_text_source() -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TRANSACTIONS_TOPIC)
        .set_group_id("flink-btc-analytics")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def kafka_sink(topic: str, transactional_prefix: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .set_transactional_id_prefix(transactional_prefix)
        .build()
    )


def main() -> None:
    env = build_env()
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(LATE_EVENT_TOLERANCE_MS))
        .with_timestamp_assigner(TxTimestampAssigner())
    )

    transactions = (
        env.from_source(
            source=kafka_text_source(),
            watermark_strategy=watermark_strategy,
            source_name="btc-transactions-source",
        )
        .map(JsonParser(), output_type=None)
        .key_by(lambda _tx: "wm")
        .process(MetricAndWatermarkEnricher(), output_type=None)
    )

    transaction_metrics = transactions.map(TransactionMetricMapper(), output_type=None)
    transaction_metrics.sink_to(kafka_sink(METRICS_TOPIC, "btc-metrics"))
    transaction_metrics.add_sink(ClickHouseJsonSink("metrics"))
    transactions.map(lambda tx: json.dumps(tx), output_type=None).add_sink(ClickHouseJsonSink("transactions"))

    tx_alerts = (
        transactions.map(TransactionAlertMapper(), output_type=None)
        .flat_map(lambda items: items, output_type=None)
    )
    tx_alerts.sink_to(kafka_sink(ALERTS_TOPIC, "btc-alerts-tx"))
    tx_alerts.add_sink(ClickHouseJsonSink("alerts"))

    address_updates = (
        transactions.map(AddressUpdateExtractor(), output_type=None)
        .flat_map(lambda items: items, output_type=None)
        .key_by(lambda item: item["address"])
        .process(AddressStateProcessor(), output_type=None)
    )

    balances = address_updates.filter(lambda row: row["kind"] == "balance", output_type=None).map(
        lambda row: row["payload"], output_type=None
    )
    state_alerts = address_updates.filter(lambda row: row["kind"] == "alert", output_type=None).map(
        lambda row: row["payload"], output_type=None
    )

    balances.add_sink(RedisBalanceSink())
    balances.add_sink(ClickHouseJsonSink("balance_updates"))
    balances.sink_to(kafka_sink(METRICS_TOPIC, "btc-metrics-balance"))

    state_alerts.sink_to(kafka_sink(ALERTS_TOPIC, "btc-alerts-state"))
    state_alerts.add_sink(ClickHouseJsonSink("alerts"))

    env.execute("bitcoin-like-streaming-analytics")


if __name__ == "__main__":
    main()
