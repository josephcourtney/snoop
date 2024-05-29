import uuid
from datetime import UTC, datetime
from typing import TypedDict

from .logging import get_logger


class ItemGroupSpec(TypedDict, total=False):
    name: str | None = None
    max_tokens: float = 10
    refill_rate: float = 1.0


class ItemGroup:
    def __init__(self, spec: ItemGroupSpec):
        self.name: str | None = spec.get("name")
        self.max_tokens: float = spec.get("max_tokens")
        self.refill_rate: float = spec.get("refill_rate")

        self.id: str = str(uuid.uuid4())
        self.last_refill_time: datetime = datetime.now(tz=UTC)
        self.last_pop: datetime = datetime.min.replace(tzinfo=UTC)
        self.tokens = self.max_tokens
        self.max_pop_rate = 1e9
        self.logger = get_logger()

    def update(self):
        current_time = datetime.now(tz=UTC)
        elapsed = (current_time - self.last_refill_time).total_seconds()
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
        self.last_refill_time = current_time
        self.logger.info(
            "refilling tokens", extra={"group_id": self.id, "tokens": self.tokens, "elapsed": elapsed}
        )

    def consume_tokens(self, quantity: int = 1) -> bool:
        self.update()
        self.logger.info("Trying to consume tokens", extra={"group": self})
        current_time = datetime.now(tz=UTC)
        elapsed = (current_time - self.last_pop).total_seconds()
        if self.tokens >= quantity and 1 / elapsed < self.max_pop_rate:
            self.tokens -= quantity
            self.logger.info(
                "consumed tokens",
                extra={
                    "group_id": self.id,
                    "n_consumed": quantity,
                    "tokens": self.tokens,
                    "elapsed": elapsed,
                },
            )
            return True
        return False

    def __iter__(self):
        """Iterate over the (property name, property value) tuples; used for serialization."""
        yield "class", type(self)
        yield from self.__dict__.items()

    def serialize(self):
        return {
            "id": self.id,
            "name": self.name,
            "tokens": self.tokens,
            "max_tokens": self.max_tokens,
            "refill_rate": self.refill_rate,
            "last_refill_time": self.last_refill_time,
            "last_pop": self.last_pop,
            "max_pop_rate": self.max_pop_rate,
        }
