import uuid
from datetime import UTC, datetime
from typing import TypedDict


class ItemGroupSpec(TypedDict, total=False):
    name: str
    max_tokens: float
    refill_rate: float


class ItemGroup:
    def __init__(self, spec: ItemGroupSpec):
        self.name: str | None = spec.get("name")
        self.max_tokens: float = spec.get("max_tokens", 10)
        self.refill_rate: float = spec.get("refill_rate", 1.0)

        self.id: str = str(uuid.uuid4())
        self.last_refill_time: datetime = datetime.now(tz=UTC)
        self.last_pop: datetime = datetime.fromtimestamp(0, tz=UTC)
        self.tokens = self.max_tokens
        self.max_pop_rate = 1e9

    def update(self):
        current_time = datetime.now(tz=UTC)
        elapsed = (current_time - self.last_refill_time).total_seconds()
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
        self.last_refill_time = current_time

    def consume_tokens(self, quantity: int = 1) -> bool:
        self.update()
        current_time = datetime.now(tz=UTC)
        elapsed = (current_time - self.last_pop).total_seconds()
        if self.tokens >= quantity and 1 / elapsed < self.max_pop_rate:
            self.tokens -= quantity
            return True
        return False

    def __iter__(self):
        """Iterate over the (property name, property value) tuples; used for serialization."""
        yield "class", type(self)
        yield from self.__dict__.items()

    def serialize(self):
        return dict(self)
