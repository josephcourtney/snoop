from collections import OrderedDict

from snoop.doop.compressor import Compressor

from .dict_kv import DictKeyValueStore


class LRUCacheKeyValueStore(DictKeyValueStore):
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
