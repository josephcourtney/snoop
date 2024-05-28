import pytest
import redis
from testcontainers.redis import RedisContainer

from snoop.doop import (
    BrotliCompressor,
    DummyCompressor,
    InMemoryKeyValueStore,
    LzmaCompressor,
    RedisKeyValueStore,
    ZlibCompressor,
    ZstdCompressor,
)


@pytest.fixture(scope="session")
def redis_container():
    # Start the Redis container at the beginning of the test session
    with RedisContainer() as container:
        yield container
        # The container will be stopped after the session ends


@pytest.fixture()
def redis_client(redis_container):
    # Use the session-scoped Redis container to create a Redis client
    redis_host = redis_container.get_container_host_ip()
    redis_port = redis_container.get_exposed_port(6379)
    client = redis.Redis(host=redis_host, port=redis_port)

    # Ensure the database is clean before the test starts
    client.flushdb()

    # Yield the client to the test
    yield client

    # Flush the database after each test
    client.flushdb()


@pytest.fixture(
    params=[
        DummyCompressor(),
        ZstdCompressor(),
        ZlibCompressor(),
        LzmaCompressor(),
        BrotliCompressor(),
    ]
)
def compressor(request):
    return request.param


@pytest.fixture(
    params=[
        InMemoryKeyValueStore,
        RedisKeyValueStore,
    ]
)
def kv_store(request, compressor, redis_client):
    if request.param is RedisKeyValueStore:
        return request.param(redis_client, compressor)
    return request.param(compressor)


def test_put_get(kv_store):
    kv_store.put("test_key", b"test_value")
    assert kv_store.get("test_key") == b"test_value"


def test_delete(kv_store):
    kv_store.put("test_key", b"test_value")
    kv_store.delete("test_key")
    assert kv_store.get("test_key") is None


def test_put_batch(kv_store):
    items = [("key1", b"value1"), ("key2", b"value2"), ("key3", b"value3")]
    kv_store.put_batch(items)
    assert kv_store.get("key1") == b"value1"
    assert kv_store.get("key2") == b"value2"
    assert kv_store.get("key3") == b"value3"


def test_get_batch(kv_store):
    items = [("key1", b"value1"), ("key2", b"value2"), ("key3", b"value3")]
    kv_store.put_batch(items)
    values = kv_store.get_batch(["key1", "key2", "key3"])
    assert values == [b"value1", b"value2", b"value3"]


def test_delete_batch(kv_store):
    items = [("key1", b"value1"), ("key2", b"value2"), ("key3", b"value3")]
    kv_store.put_batch(items)
    kv_store.delete_batch(["key1", "key2", "key3"])
    assert kv_store.get("key1") is None
    assert kv_store.get("key2") is None
    assert kv_store.get("key3") is None
