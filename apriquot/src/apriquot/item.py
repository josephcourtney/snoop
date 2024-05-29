import datetime
import random
import uuid
from enum import auto
from typing import Any, TypedDict

from .logging import get_logger
from .ordered_enum import OrderedEnum


class RetryLimitExceededError(ValueError):
    """Raised when an item has exceeded its maximum number of retries."""


class ItemState(OrderedEnum):
    IMMATURE = auto()
    READY = auto()
    IN_PROGRESS = auto()
    EXPIRED = auto()
    FAILED = auto()
    COMPLETED = auto()


class ItemSpec(TypedDict, total=False):
    item: Any
    priority: float  # base priority value assigned to the item. must be non-negative.
    cost: int  # cost of popping the item in terms of tokens consumed from group bucket.
    aging_factor: float  # factor by which the effective priority is decreased each second
    matures: datetime.datetime  # the earliest time an item can be successfully popped
    deadline: datetime.datetime  # the latest time an item can be successfully popped
    max_retries: int  # the maximum number of retry attempts
    backoff_factor: float  # how much the delay before a task is retried increases on each attempt
    base_retry_delay: float  # the length of delay before a task is retried the first time
    jitter: float  # the amplitude of the random adjustment to retry delays as a fraction of the delay time
    group: str  # an identifier for the group and item belongs to
    dependencies: list[str]  # List of item IDs that this item depends on
    minimum_fractional_priority: float


class Item:
    def __init__(self, spec: ItemSpec):
        """
        Initialize an Item with the given specifications.

        Args:
            spec (ItemSpec): Specifications for the item.
        """
        self.item: Any = spec["item"]
        self.priority: float = spec["priority"]
        self.minimum_fractional_priority: float = spec.get("minimum_fractional_priority", 0.1)
        self.cost: int = spec.get("cost", 1)
        self.aging_factor: float = spec.get("aging_factor", 0.9)
        self.enqueued = datetime.datetime.now(tz=datetime.UTC)
        self.matures: datetime.datetime = spec.get("matures") or self.enqueued
        self.deadline: datetime.datetime = spec.get("deadline") or self.enqueued + datetime.timedelta(
            weeks=52
        )
        self.max_retries: int = spec.get("max_retries", 3)
        self.backoff_factor: float = spec.get("backoff_factor", 2.0)
        self.base_retry_delay: float = spec.get("base_retry_delay", 0.1)
        self.jitter: float = spec.get("jitter", 0.1)
        self.group: str | None = spec.get("group")
        self.dependencies: list[str] = spec.get(
            "dependencies", []
        )  # List of item IDs that this item depends on

        self.logger = get_logger()

        self.id: str = str(uuid.uuid4())
        self.retries: int = 0
        self.last_popped: datetime.datetime | None = None

        if self.priority < 0:
            msg = "Priority must be a non-negative integer"
            raise ValueError(msg)

        if self.jitter < 0 or self.jitter > 1:
            msg = "jitter must be in [0, 1]"
            raise ValueError(msg)

        self.update_mature_time()

        self.state: ItemState = ItemState.IMMATURE if self.matures is None else ItemState.READY

    def update_mature_time(self):
        if self.retries > 0:
            jitter = random.uniform(-self.jitter / 2, self.jitter / 2)  # noqa: S311
            delay = self.base_retry_delay * (self.backoff_factor**self.retries) * (1 + jitter)
            earliest_retry = (self.last_popped or self.enqueued) + datetime.timedelta(seconds=delay)
            self.matures = max(self.matures, earliest_retry)

            self.logger.error(
                "maturation time updated",
                extra={"item_id": self.id, "maturation": self.matures, "delay": delay},
            )

    @property
    def age(self):
        return (datetime.datetime.now(tz=datetime.UTC) - self.enqueued).total_seconds()

    @property
    def effective_priority(self):
        effective_priority = self.priority * self.aging_factor**self.age
        if self.deadline:
            effective_priority *= 1 - self.age / (self.deadline - self.enqueued).total_seconds()
        return (1 - self.minimum_fractional_priority) * effective_priority + self.minimum_fractional_priority

    def retry(self):
        self.retries += 1
        if self.retries > self.max_retries:
            msg = f"No more retries left for item {self.id}"
            raise RetryLimitExceededError(msg)
        self.update_mature_time()

    def __lt__(self, other: "Item") -> bool:
        """Compare this item with another item based on effective priority."""
        return self.effective_priority < other.effective_priority

    def __iter__(self):
        """Iterate over (property name, property value) tuples, for serialization."""
        return iter({"class": type(self), **self.__dict__}.items())

    def serialize(self):
        return dict(self)
