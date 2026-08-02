from __future__ import annotations

from collections.abc import Iterator

from app.models import RawTransactionEvent
from app.sources.base import TransactionSource


class BitcoinCoreSource(TransactionSource):
    """
    Placeholder for future RPC/ZMQ-backed ingestion.

    Expected evolution points:
    - RPC polling: getrawmempool, getrawtransaction, getblock
    - ZMQ listeners for mempool and block notifications
    - Mempool replay and backfill coordination
    """

    def __init__(self, *_args, **_kwargs) -> None:
        self.capabilities = {
            "rpc_methods": ["getrawmempool", "getrawtransaction", "getblock"],
            "transport": ["rpc", "zmq"],
        }

    def stream(self) -> Iterator[RawTransactionEvent]:
        raise NotImplementedError(
            "BitcoinCoreSource is intentionally a placeholder. "
            "Implement RPC/ZMQ ingestion here without changing downstream normalization or Flink."
        )
