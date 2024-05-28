import redis

from snoop.doop.compressor import Compressor

from .abc import KeyValueStore


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
