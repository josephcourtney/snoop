from snoop.doop.compressor import Compressor

from .abc import KeyValueStore
from .lru_kv import LRUCacheKeyValueStore


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
