from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict

import redis

from .compressor import Compressor, DummyCompressor


class KeyValueStore(ABC):
    def __init__(self, compressor: Compressor | None):
        self.compressor = DummyCompressor() if compressor is None else compressor
        self.reference_count: dict[str, int] = defaultdict(int)
        self.store: dict[str, bytes] = {}

    @abstractmethod
    def _put(self, key: str, value: bytes) -> None:
        """Put a key-value pair into the store.

        Internal implementation - value is already compressed.
        """

    @abstractmethod
    def _get(self, key: str) -> bytes:
        """Get a value from the store by key.

        Internal implementation - returned value will be decompressed.
        """

    @abstractmethod
    def _delete(self, key: str) -> None:
        """Delete a key-value pair from the store.

        Internal implementation - currently public function is a pass-through to this function.
        """

    def put(self, key: str, value: bytes) -> None:
        """Put a key-value pair into the store."""
        compressed_value = self.compressor.compress(value)
        self._put(key, compressed_value)

    def get(self, key: str) -> bytes:
        """Get a value from the store by key."""
        compressed_value = self._get(key)
        if compressed_value is not None:
            return self.compressor.decompress(compressed_value)
        return None

    def delete(self, key: str) -> None:
        """Delete a key-value pair from the store."""
        self._delete(key)

    def put_batch(self, items: list[tuple[str, bytes]]) -> None:
        for key, value in items:
            self.put(key, value)

    def get_batch(self, keys: list[str]) -> list[bytes]:
        return [self.get(key) for key in keys]

    def delete_batch(self, keys: list[str]) -> None:
        for key in keys:
            self.delete(key)


class InMemoryKeyValueStore(KeyValueStore):
    """In-memory key-value store implementation backed by a python dict."""

    def _put(self, key: str, value: bytes) -> None:
        if key not in self.store:
            self.store[key] = value
            self.reference_count[key] = 1
        else:
            self.reference_count[key] += 1

    def _get(self, key: str) -> bytes:
        return self.store.get(key)

    def _delete(self, key: str) -> None:
        if key in self.store:
            self.reference_count[key] -= 1
            if self.reference_count[key] == 0:
                del self.store[key]
                del self.reference_count[key]


class RedisKeyValueStore(KeyValueStore):
    """Redis-based key-value store implementation."""

    def __init__(self, redis_client: redis.Redis, compressor: Compressor | None):
        super().__init__(compressor)
        self.redis_client = redis_client

    def _put(self, key: str, value: bytes) -> None:
        if not self.redis_client.exists(key):
            self.redis_client.set(key, value)
            self.reference_count[key] = 1
        else:
            self.reference_count[key] += 1

    def _get(self, key: str) -> bytes:
        return self.redis_client.get(key)

    def _delete(self, key: str) -> None:
        if self.redis_client.exists(key):
            self.reference_count[key] -= 1
            if self.reference_count[key] == 0:
                self.redis_client.delete(key)
                del self.reference_count[key]


class LRUCacheKeyValueStore(InMemoryKeyValueStore):
    """In-memory key-value store with LRU eviction strategy."""

    def __init__(self, max_size: int, compressor: Compressor | None = None):
        super().__init__(compressor)
        self.max_size = max_size
        self.cache = OrderedDict()

    def _put(self, key: str, value: bytes) -> None:
        if key in self.cache:
            # Move the existing key to the end to mark it as recently used
            self.cache.move_to_end(key)
        self.cache[key] = value
        super()._put(key, value)

        if len(self.cache) > self.max_size:
            # Evict the least recently used item
            oldest_key = next(iter(self.cache))
            self._delete(oldest_key)

    def _get(self, key: str) -> bytes:
        if key in self.cache:
            # Move the accessed key to the end to mark it as recently used
            self.cache.move_to_end(key)
            return self.cache[key]
        return super()._get(key)

    def _delete(self, key: str) -> None:
        if key in self.cache:
            del self.cache[key]
        super()._delete(key)


# Usage
class HybridKeyValueStore(KeyValueStore):
    """Hybrid key-value store combining local in-memory cache with remote store."""

    def __init__(
        self,
        remote_store: KeyValueStore,
        local_store: KeyValueStore | None = None,
        compressor: Compressor | None = None,
    ):
        super().__init__(compressor)
        self.local_store = LRUCacheKeyValueStore if local_store is None else local_store
        self.remote_store = remote_store

    def _put(self, key: str, value: bytes) -> None:
        self.local_store._put(key, value)  # noqa: SLF001
        self.remote_store._put(key, value)  # noqa: SLF001

    def _get(self, key: str) -> bytes:
        value = self.local_store._get(key)  # noqa: SLF001

        if value is None:
            value = self.remote_store._get(key)  # noqa: SLF001

            if value is not None:
                self.local_store._put(key, value)  # noqa: SLF001

        return value

    def _delete(self, key: str) -> None:
        self.local_store._delete(key)  # noqa: SLF001
        self.remote_store._delete(key)  # noqa: SLF001
