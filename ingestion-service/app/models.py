from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(slots=True)
class RawTransactionInput:
    address: str
    value: float
    prev_tx_id: str
    prev_output_index: int


@dataclass(slots=True)
class RawTransactionOutput:
    address: str
    value: float
    output_index: int


@dataclass(slots=True)
class RawTransactionEvent:
    tx_id: str
    timestamp: int
    inputs: list[RawTransactionInput]
    outputs: list[RawTransactionOutput]
    source: str
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NormalizedTransactionInput:
    address: str
    value: float
    prev_tx_id: str
    prev_output_index: int


@dataclass(slots=True)
class NormalizedTransactionOutput:
    address: str
    value: float
    output_index: int


@dataclass(slots=True)
class NormalizedTransactionEvent:
    tx_id: str
    timestamp: int
    inputs: list[NormalizedTransactionInput]
    outputs: list[NormalizedTransactionOutput]
    source: str
    ingest_time: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
