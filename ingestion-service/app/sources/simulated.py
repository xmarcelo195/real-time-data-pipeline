from __future__ import annotations

import os
import random
import time
import uuid
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass

from app.models import RawTransactionEvent, RawTransactionInput, RawTransactionOutput
from app.sources.base import TransactionSource


@dataclass(slots=True)
class Utxo:
    tx_id: str
    output_index: int
    address: str
    value: float
    timestamp: int


class SimulatedSource(TransactionSource):
    def __init__(self) -> None:
        seed = int(os.getenv("SIMULATION_SEED", "42"))
        self.random = random.Random(seed)
        self.address_count = int(os.getenv("SIM_ADDRESS_COUNT", "5000"))
        self.target_tps = float(os.getenv("SIM_TARGET_TPS", "40"))
        self.max_lateness_ms = int(os.getenv("SIM_MAX_LATENESS_MS", "15000"))
        self.min_tx_value = float(os.getenv("SIM_MIN_TX_VALUE", "0.0005"))
        self.structuring_ceiling = float(os.getenv("SIM_STRUCTURING_CEILING", "0.99"))
        self.whale_threshold = float(os.getenv("SIM_WHALE_THRESHOLD", "50"))
        self.clock_ms = int(time.time() * 1000)
        self.tx_sequence = 0
        self.addresses = [f"addr_{index:05d}" for index in range(self.address_count)]
        self.address_utxos: dict[str, list[Utxo]] = defaultdict(list)
        self._bootstrap_utxos()

    def _bootstrap_utxos(self) -> None:
        for _ in range(self.address_count * 2):
            address = self.random.choice(self.addresses)
            value = round(self.random.uniform(0.25, 3.5), 8)
            tx_id = self._new_tx_id(prefix="genesis")
            self.address_utxos[address].append(
                Utxo(
                    tx_id=tx_id,
                    output_index=0,
                    address=address,
                    value=value,
                    timestamp=self.clock_ms - self.random.randint(1000, 60000),
                )
            )

        whale_addresses = self.random.sample(self.addresses, max(3, self.address_count // 250))
        for address in whale_addresses:
            tx_id = self._new_tx_id(prefix="whale")
            self.address_utxos[address].append(
                Utxo(
                    tx_id=tx_id,
                    output_index=0,
                    address=address,
                    value=round(self.random.uniform(90, 180), 8),
                    timestamp=self.clock_ms,
                )
            )

    def _new_tx_id(self, prefix: str = "sim") -> str:
        self.tx_sequence += 1
        return f"{prefix}-{self.tx_sequence:09d}-{uuid.uuid4().hex[:16]}"

    def _pick_address_with_balance(self, minimum_total: float) -> tuple[str, list[Utxo], float]:
        candidates = self.addresses[:]
        self.random.shuffle(candidates)
        for address in candidates:
            utxos = self.address_utxos[address]
            if not utxos:
                continue
            running = 0.0
            selected: list[Utxo] = []
            for utxo in sorted(utxos, key=lambda item: item.value, reverse=True):
                selected.append(utxo)
                running += utxo.value
                if running >= minimum_total:
                    return address, selected, running
        raise RuntimeError("Simulator could not find enough spendable UTXOs.")

    def _consume_utxos(self, address: str, spent: list[Utxo]) -> None:
        spent_keys = {(item.tx_id, item.output_index) for item in spent}
        self.address_utxos[address] = [
            item
            for item in self.address_utxos[address]
            if (item.tx_id, item.output_index) not in spent_keys
        ]

    def _allocate_outputs(
        self,
        tx_id: str,
        timestamp: int,
        outputs: list[tuple[str, float]],
    ) -> list[RawTransactionOutput]:
        raw_outputs: list[RawTransactionOutput] = []
        for index, (address, value) in enumerate(outputs):
            rounded = round(value, 8)
            raw_outputs.append(
                RawTransactionOutput(address=address, value=rounded, output_index=index)
            )
            self.address_utxos[address].append(
                Utxo(
                    tx_id=tx_id,
                    output_index=index,
                    address=address,
                    value=rounded,
                    timestamp=timestamp,
                )
            )
        return raw_outputs

    def _normal_tx(self) -> RawTransactionEvent:
        spend_target = self.random.uniform(self.min_tx_value, 2.0)
        address, selected, total_input = self._pick_address_with_balance(spend_target)
        destination = self.random.choice([addr for addr in self.addresses if addr != address])
        payment = round(min(spend_target, total_input * self.random.uniform(0.4, 0.85)), 8)
        fee = round(total_input * self.random.uniform(0.0002, 0.001), 8)
        change = round(max(total_input - payment - fee, 0), 8)
        tx_id = self._new_tx_id()
        self._consume_utxos(address, selected)
        outputs = [(destination, payment)]
        if change > 0:
            outputs.append((address, change))
        return self._build_event(tx_id, selected, outputs)

    def _fan_out_tx(self) -> RawTransactionEvent:
        spend_target = self.random.uniform(3, 9)
        address, selected, total_input = self._pick_address_with_balance(spend_target)
        output_count = self.random.randint(8, 20)
        fee = round(total_input * self.random.uniform(0.0008, 0.0015), 8)
        distributable = max(total_input - fee, self.min_tx_value)
        shard_value = round(distributable / (output_count + 1), 8)
        recipients = self.random.sample([addr for addr in self.addresses if addr != address], output_count)
        outputs = [(recipient, shard_value) for recipient in recipients]
        change = round(total_input - shard_value * output_count - fee, 8)
        if change > 0:
            outputs.append((address, change))
        tx_id = self._new_tx_id(prefix="fanout")
        self._consume_utxos(address, selected)
        return self._build_event(tx_id, selected, outputs)

    def _consolidation_tx(self) -> RawTransactionEvent:
        address, selected, total_input = self._pick_address_with_balance(1.5)
        if len(selected) < 3:
            return self._normal_tx()
        recipient = self.random.choice([addr for addr in self.addresses if addr != address])
        fee = round(total_input * self.random.uniform(0.0003, 0.001), 8)
        outputs = [(recipient, round(total_input - fee, 8))]
        tx_id = self._new_tx_id(prefix="consolidation")
        self._consume_utxos(address, selected)
        return self._build_event(tx_id, selected, outputs)

    def _whale_tx(self) -> RawTransactionEvent:
        address, selected, total_input = self._pick_address_with_balance(self.whale_threshold)
        payout_count = self.random.randint(2, 5)
        recipients = self.random.sample([addr for addr in self.addresses if addr != address], payout_count)
        fee = round(total_input * self.random.uniform(0.001, 0.002), 8)
        dominant = round(total_input * self.random.uniform(0.65, 0.85), 8)
        remainder = round(max(total_input - dominant - fee, 0), 8)
        outputs = [(recipients[0], dominant)]
        if payout_count > 1 and remainder > 0:
            split = round(remainder / (payout_count - 1), 8)
            outputs.extend((recipient, split) for recipient in recipients[1:])
        tx_id = self._new_tx_id(prefix="whale")
        self._consume_utxos(address, selected)
        return self._build_event(tx_id, selected, outputs)

    def _structured_tx(self) -> RawTransactionEvent:
        spend_target = self.random.uniform(4, 12)
        address, selected, total_input = self._pick_address_with_balance(spend_target)
        payment_count = self.random.randint(4, 9)
        recipients = self.random.sample([addr for addr in self.addresses if addr != address], payment_count)
        outputs = [
            (recipient, round(self.random.uniform(0.55, self.structuring_ceiling), 8))
            for recipient in recipients
        ]
        spent_value = sum(value for _, value in outputs)
        fee = round(total_input * self.random.uniform(0.0002, 0.0008), 8)
        change = round(max(total_input - spent_value - fee, 0), 8)
        if change > 0:
            outputs.append((address, change))
        tx_id = self._new_tx_id(prefix="structured")
        self._consume_utxos(address, selected)
        return self._build_event(tx_id, selected, outputs)

    def _build_event(
        self,
        tx_id: str,
        spent_utxos: list[Utxo],
        outputs: list[tuple[str, float]],
    ) -> RawTransactionEvent:
        inter_arrival_ms = max(1, int(1000 / max(self.target_tps, 1)))
        self.clock_ms += self.random.randint(1, inter_arrival_ms * 2)
        event_timestamp = self.clock_ms - self.random.randint(0, self.max_lateness_ms)
        inputs = [
            RawTransactionInput(
                address=item.address,
                value=round(item.value, 8),
                prev_tx_id=item.tx_id,
                prev_output_index=item.output_index,
            )
            for item in spent_utxos
        ]
        raw_outputs = self._allocate_outputs(tx_id=tx_id, timestamp=event_timestamp, outputs=outputs)
        return RawTransactionEvent(
            tx_id=tx_id,
            timestamp=event_timestamp,
            inputs=inputs,
            outputs=raw_outputs,
            source="simulated",
        )

    def _next_event(self) -> RawTransactionEvent:
        roll = self.random.random()
        if roll < 0.55:
            return self._normal_tx()
        if roll < 0.72:
            return self._fan_out_tx()
        if roll < 0.84:
            return self._structured_tx()
        if roll < 0.93:
            return self._consolidation_tx()
        return self._whale_tx()

    def stream(self) -> Iterator[RawTransactionEvent]:
        while True:
            event = self._next_event()
            yield event
            delay = max(0.002, 1 / max(self.target_tps, 1))
            time.sleep(self.random.uniform(delay / 4, delay))
