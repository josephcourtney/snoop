from .abc import KeyValueStore


class DictKeyValueStore(KeyValueStore):
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
