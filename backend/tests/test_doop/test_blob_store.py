import hashlib
from unittest.mock import MagicMock

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from snoop.doop.blob_store import (
    Blob,
    BlobCorruptedError,
    BlobExistsError,
    BlobNotFoundError,
    BlobStore,
    Chunk,
)


@pytest.fixture()
def sqlite_engine():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    return engine


@pytest.fixture()
def chunker():
    mock_chunker = MagicMock()
    mock_chunker.chunk_blob.side_effect = lambda blob_data: [
        (hashlib.sha256(chunk).hexdigest(), chunk)
        for chunk in [blob_data[i : i + 10] for i in range(0, len(blob_data), 10)]
    ]
    return mock_chunker


@pytest.fixture()
def kv_store():
    return MagicMock()


@pytest.fixture()
def blob_store(sqlite_engine, chunker, kv_store):
    return BlobStore(db_engine=sqlite_engine, chunker=chunker, kv_store=kv_store)


def test_store_blob_success(blob_store, kv_store):
    identifier = "test_blob"
    blob_data = b"This is a test blob." * 10

    blob_store.store_blob(identifier, blob_data)

    # Check if the blob was stored in the kv_store
    chunk_items = blob_store.chunker.chunk_blob(blob_data)
    kv_store.put_batch.assert_called_once_with(chunk_items)


def test_store_blob_exists(blob_store, kv_store):
    identifier = "test_blob"
    blob_data = b"This is a test blob." * 10

    blob_store.store_blob(identifier, blob_data)

    with pytest.raises(BlobExistsError):
        blob_store.store_blob(identifier, blob_data)


def test_retrieve_blob_success(blob_store, kv_store):
    identifier = "test_blob"
    blob_data = b"This is a test blob." * 10

    blob_store.store_blob(identifier, blob_data)

    # Mock the kv_store.get_batch to return the correct chunks
    chunk_items = blob_store.chunker.chunk_blob(blob_data)
    kv_store.get_batch.return_value = [chunk for _, chunk in chunk_items]

    retrieved_blob = blob_store.retrieve_blob(identifier)

    assert retrieved_blob == blob_data


def test_retrieve_blob_not_found(blob_store):
    with pytest.raises(BlobNotFoundError):
        blob_store.retrieve_blob("non_existent_blob")


def test_retrieve_blob_corrupted(blob_store, kv_store):
    identifier = "test_blob"
    blob_data = b"This is a test blob." * 10

    blob_store.store_blob(identifier, blob_data)

    # Mock the kv_store.get_batch to return corrupted data
    chunk_items = blob_store.chunker.chunk_blob(blob_data)
    corrupted_data = [b"corrupted data" for _ in chunk_items]
    kv_store.get_batch.return_value = corrupted_data

    with pytest.raises(BlobCorruptedError):
        blob_store.retrieve_blob(identifier)


def test_delete_blob_success(blob_store, kv_store):
    identifier = "test_blob"
    blob_data = b"This is a test blob." * 10

    blob_store.store_blob(identifier, blob_data)

    blob_store.delete_blob(identifier)

    with pytest.raises(BlobNotFoundError):
        blob_store.retrieve_blob(identifier)

    # Check if the chunks were deleted from kv_store
    chunk_items = blob_store.chunker.chunk_blob(blob_data)
    chunk_keys = list({hash_key for hash_key, _ in chunk_items})
    kv_store.delete_batch.assert_called_once_with(chunk_keys)


def test_delete_blob_not_found(blob_store):
    with pytest.raises(BlobNotFoundError):
        blob_store.delete_blob("non_existent_blob")


def test_store_blob_integrity_check(blob_store, kv_store):
    identifier = "test_blob"
    blob_data = b"This is a test blob." * 10

    # Mock the kv_store.put_batch to simulate a failure
    kv_store.put_batch.side_effect = Exception("Storage error")

    with pytest.raises(Exception, match="Storage error"):
        blob_store.store_blob(identifier, blob_data)

    # Verify that no blob was stored in the database
    with Session(blob_store.engine) as session:
        blob = session.exec(select(Blob).where(Blob.identifier == identifier)).first()
        assert blob is None

    # Verify that no chunks were stored in the database
    with Session(blob_store.engine) as session:
        chunks = session.exec(select(Chunk)).all()
        assert len(chunks) == 0
