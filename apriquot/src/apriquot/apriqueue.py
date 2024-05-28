import asyncio
import heapq
import json
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Self, Unpack

from .item import Item, ItemSpec, ItemState
from .item_group import ItemGroup, ItemGroupSpec
from .logging import get_logger


class ItemExpiredError(ValueError):
    """Raised when an item is pushed with a deadline that has already passed."""


class QueueEmptyError(ValueError):
    """Raised when there are no eligible items to pop from the queue."""


class ApriQueue:
    def __init__(self: Self) -> None:
        self.priority_heap: list[Item] = []
        self.maturation_heap: list[tuple[datetime, Item]] = []
        self.expiration_heap: list[tuple[datetime, Item]] = []

        self.item_map: dict[str, Item] = {}

        self.new_item_event = asyncio.Event()
        self.lock = threading.Lock()
        self.groups: dict[str, ItemGroup] = {}
        self.logger = get_logger()
        self.group_defaults: ItemGroupSpec = ItemGroupSpec(max_tokens=10, refill_rate=1.0)
        self.completed_items: set[str] = set()
        self.failed_items: set[str] = set()
        self.logger.info("ApriQueue initialized")

    def new_group(self, **kwargs: Unpack[ItemGroupSpec]) -> str:
        with self.lock:
            new_group = ItemGroup(self.group_defaults | kwargs)
            self.groups[new_group.id] = new_group
            self.logger.debug("Group created", extra={"group": new_group.serialize()})
        return new_group.id

    def has_cyclic_dependency(self, item_id: str, dependencies: list[str]) -> bool:
        visited = set()
        stack = set()

        def visit(node: str) -> bool:
            if node in stack:
                return True
            if node in visited:
                return False
            stack.add(node)
            visited.add(node)
            for dep in self.item_map[node].dependencies:
                if visit(dep):
                    return True
            stack.remove(node)
            return False

        stack.add(item_id)
        visited.add(item_id)
        for dep in dependencies:
            if visit(dep):
                return True
        stack.remove(item_id)
        return False

    def push(self, **kwargs: Unpack[ItemSpec]) -> str:
        new_item = Item(kwargs)
        current_time = datetime.now(tz=UTC)

        self.logger.debug(
            "Pushing item",
            extra={
                "item": new_item.serialize(),
                "queue": self.serialize(),
            },
        )

        if new_item.deadline and new_item.deadline < current_time:
            msg = f"Item {new_item.id} has already expired"
            self.logger.error("Item has already expired", extra={"item": new_item.serialize()})
            raise ItemExpiredError(msg)

        if new_item.matures and new_item.deadline and new_item.deadline < new_item.matures:
            msg = f"Item {new_item.id} expires before maturation"
            self.logger.error("Item expires before maturation", extra={"item": new_item.serialize()})
            raise ValueError(msg)

        if self.has_cyclic_dependency(new_item.id, new_item.dependencies):
            msg = f"Adding item {new_item.id} would create a cyclic dependency"
            self.logger.error("Cyclic dependency detected", extra={"item": new_item.serialize()})
            raise ValueError(msg)

        with self.lock:
            if new_item.matures and new_item.matures > current_time:
                heapq.heappush(self.maturation_heap, (new_item.matures, new_item))
                self.logger.debug("Item added to the maturation heap", extra={"item": new_item.serialize()})
            else:
                heapq.heappush(self.priority_heap, new_item)
                self.logger.debug("Item added to the priority heap", extra={"item": new_item.serialize()})
            if new_item.deadline:
                heapq.heappush(self.expiration_heap, (new_item.deadline, new_item))
                self.logger.debug("Item added to the expiration heap", extra={"item": new_item.serialize()})
            self.item_map[new_item.id] = new_item
            self.logger.info(
                "Item successfully pushed", extra={"item": new_item.serialize(), "queue": self.serialize()}
            )

        return new_item.id

    def retry_item(self, item_id: str) -> bool:
        item = self.item_map[item_id]
        item.retry()

        with self.lock:
            if item.matures:
                heapq.heappush(self.maturation_heap, (item.matures, item))
            self.logger.info("Item retried", extra={"item": item.serialize()})
            if item.retries >= item.max_retries:
                return False
            if item.retries >= item.max_retries - 1:
                self.logger.warning("Item on last retry", extra={"item": item.serialize()})
            return True

    def pop(self) -> Item:
        with self.lock:
            self.logger.info("Attempting to pop", extra={"queue": self.serialize()})
            if not self.priority_heap and not self.maturation_heap:
                msg = "Pop from an empty priority heap"
                self.logger.warning("Pop from an empty priority heap")
                raise QueueEmptyError(msg)

            self.logger.debug(
                "Attempting to pop",
                extra={
                    "queue": self.serialize(),
                },
            )

            self._move_matured_items_to_priority_heap()
            self._remove_expired_items()
            self._update_priorities()

            item = self._pop_next_eligible_item()
            self.logger.info("Item successfully popped", extra={"item": item.serialize()})
            return item

    def _move_matured_items_to_priority_heap(self) -> None:
        current_time = datetime.now(tz=UTC)
        while self.maturation_heap and self.maturation_heap[0][0] <= current_time:
            _, matured_item = heapq.heappop(self.maturation_heap)
            matured_item.state = ItemState.READY
            heapq.heappush(self.priority_heap, matured_item)
            self.logger.debug(
                "Item has matured and moved to priority heap", extra={"item": matured_item.serialize()}
            )

    def _remove_expired_items(self) -> None:
        current_time = datetime.now(tz=UTC)
        while self.expiration_heap and self.expiration_heap[0][0] <= current_time:
            _, expired_item = heapq.heappop(self.expiration_heap)
            expired_item.state = ItemState.EXPIRED
            self.logger.debug(
                "Item has expired and removed from priority heap", extra={"item": expired_item.serialize()}
            )

    def _update_priorities(self) -> None:
        self.priority_heap = [item for item in self.priority_heap if item.state != ItemState.READY]
        heapq.heapify(self.priority_heap)

    def _pop_next_eligible_item(self) -> Item:
        current_time = datetime.now(tz=UTC)
        item = None
        to_requeue = []
        while self.priority_heap:
            item = heapq.heappop(self.priority_heap)
            if item.state == ItemState.EXPIRED:
                self.logger.debug(
                    "Item is expired and will be removed from priority heap.",
                    extra={"item": item.serialize()},
                )
                continue
            if item.state == ItemState.IMMATURE:
                if item.matures > current_time:
                    self.logger.debug(
                        "Item is immature and will be moved back to maturation heap.",
                        extra={"item": item.serialize()},
                    )
                    heapq.heappush(self.maturation_heap, (item.matures, item))
                    continue
                if item.matures is None:
                    item.state = ItemState.READY
                    self.logger.info(
                        "Item marked as immature but it does not have a maturation time.",
                        extra={"item": item.serialize()},
                    )
                else:
                    item.state = ItemState.READY
                    self.logger.info(
                        "Item marked as immature but it is after its maturation time.",
                        extra={"item": item.serialize()},
                    )

            if any(dep not in self.completed_items for dep in item.dependencies):
                self.logger.debug("Item has unmet dependencies.", extra={"item": item.serialize()})
                to_requeue.append(item)
                continue

            if (group := self.groups.get(item.group)) is None and not group.consume_tokens(item.cost):
                self.logger.debug(
                    "Item's group has insufficient tokens.", extra={"item": item, "group": group.serialize()}
                )
                to_requeue.append(item)
                continue
            break

        for e in to_requeue:
            heapq.heappush(self.priority_heap, e)

        if item is None:
            msg = "No eligible items to pop at the current time"
            self.logger.debug(msg, extra={"queue": self.serialize()})
            raise QueueEmptyError(msg)
        item.state = ItemState.IN_PROGRESS
        item.last_popped = current_time
        self.logger.debug("Item popped from queue and is now in progress", extra={"item": item.serialize()})
        return item

    def save(self, file_path: Path | str) -> None:
        with self.lock, Path(file_path).open("w", encoding="utf-8") as f:
            json.dump(
                [
                    dict(item)
                    for item in self.priority_heap
                    + [x[1] for x in self.maturation_heap]
                    + [x[1] for x in self.expiration_heap]
                ],
                f,
            )
        self.logger.info("Queue state saved to file", extra={"file_path": file_path})

    def load(self, file_path: Path | str) -> None:
        with self.lock, Path(file_path).open(encoding="utf-8") as f:
            items = [Item(**item) for item in json.load(f)]
            self.priority_heap = []
            self.maturation_heap = []
            self.expiration_heap = []
            self.item_map = {}
            for item in items:
                self.item_map[item.id] = item
                if item.deadline and item.deadline > datetime.now(tz=UTC):
                    heapq.heappush(self.expiration_heap, (item.deadline, item))
                if item.matures and item.matures > datetime.now(tz=UTC):
                    heapq.heappush(self.maturation_heap, (item.matures, item))
                else:
                    heapq.heappush(self.priority_heap, item)
            self.logger.info("Queue state loaded from file", extra={"file_path": file_path})

    def mark_complete(self, item_id: str) -> None:
        with self.lock:
            if item_id in self.item_map:
                item = self.item_map[item_id]
                item.state = ItemState.COMPLETED
                self.completed_items.add(item_id)
                self.logger.debug("Item marked as completed", extra={"item": self.item_map[item_id]})
            else:
                self.logger.warning(
                    "Item unknown. Cannot marked as completed", extra={"item": self.item_map[item_id]}
                )

    def mark_failed(self, item_id: str) -> None:
        with self.lock:
            if item_id in self.item_map:
                if not self.retry_item(item_id):
                    item = self.item_map[item_id]
                    item.state = ItemState.FAILED
                    self.failed_items.add(item_id)
                    self.logger.debug("Item marked as failed", extra={"item": self.item_map[item_id]})
            else:
                self.logger.warning(
                    "Item unknown. Cannot marked as failed", extra={"item": self.item_map[item_id]}
                )

    def __iter__(self):
        """Iterate over (property name, property value) tuples, for serialization."""
        yield "class", type(self)
        yield from self.__dict__.items()

    def serialize(self):
        return {
            "maturation_heap": [e.serialize() for _, e in self.maturation_heap],
            "priority_heap": [e.serialize() for e in self.priority_heap],
            "expiration_heap": [e.serialize() for _, e in self.expiration_heap],
            "groups": {k: v.serialize() for k, v in self.groups.items()},
            "completed_items": list(self.completed_items),
            "failed_items": list(self.failed_items),
        }
