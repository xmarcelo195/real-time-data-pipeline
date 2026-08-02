from __future__ import annotations

import time

from app.models import (
    NormalizedTransactionEvent,
    NormalizedTransactionInput,
    NormalizedTransactionOutput,
    RawTransactionEvent,
)


def normalize_transaction(event: RawTransactionEvent) -> NormalizedTransactionEvent:
    return NormalizedTransactionEvent(
        tx_id=event.tx_id,
        timestamp=event.timestamp,
        inputs=[
            NormalizedTransactionInput(
                address=item.address,
                value=round(item.value, 8),
                prev_tx_id=item.prev_tx_id,
                prev_output_index=item.prev_output_index,
            )
            for item in event.inputs
        ],
        outputs=[
            NormalizedTransactionOutput(
                address=item.address,
                value=round(item.value, 8),
                output_index=item.output_index,
            )
            for item in event.outputs
        ],
        source=event.source,
        ingest_time=int(time.time() * 1000),
    )
