import pytest
import redis
from sqlmodel import create_engine
from testcontainers.redis import RedisContainer

from snoop.doop import DictKeyValueStore, FastContentDefinedChunker, FixedSizeChunker, RedisKeyValueStore


@pytest.fixture(scope="module")
def redis_client():
    with RedisContainer() as redis_container:
        yield redis.Redis(
            host=redis_container.get_container_host_ip(),
            port=redis_container.get_exposed_port(6379),
            db=0,
            decode_responses=False,
        )


@pytest.fixture()
def dict_store():
    return DictKeyValueStore()


@pytest.fixture()
def redis_store(redis_client):
    return RedisKeyValueStore(redis_client)


@pytest.fixture()
def fixed_size_chunker():
    return FixedSizeChunker(chunk_size=32)


@pytest.fixture()
def fast_content_defined_chunker():
    return FastContentDefinedChunker(min_chunk_size=16, avg_chunk_size=32, max_chunk_size=64, mask=0x1F)


@pytest.fixture()
def sqlite_engine():
    return create_engine("sqlite:///:memory:")
