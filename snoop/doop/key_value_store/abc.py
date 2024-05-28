from abc import ABC, abstractmethod
from collections import defaultdict

from snoop.doop.compressor import Compressor, DummyCompressor


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
