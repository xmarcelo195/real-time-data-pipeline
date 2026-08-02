from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator

from app.models import RawTransactionEvent


class TransactionSource(ABC):
    @abstractmethod
    def stream(self) -> Iterator[RawTransactionEvent]:
        """Yield raw transaction events from any upstream source."""
