from pathlib import Path

import pytest
from sqlalchemy.engine import Engine
from sqlmodel import SQLModel, create_engine

from snoop.doop import ChunkStore, DictStore, FixedSizeChunker
from snoop.doop import MetaDB as DoopMetaDB
from snoop.metadb.manager import MetaDB


@pytest.fixture(scope="module")
def test_db():
    # Setup: Create a new test database
    test_db_path = Path("test_binary_data.db")
    test_db_url = f"sqlite:///{test_db_path}"
    db = MetaDB(test_db_url)
    yield db
    # Teardown: Remove the test database file
    db.close()
    if test_db_path.exists():
        test_db_path.unlink()


# doop
@pytest.fixture()
def engine() -> Engine:
    engine = create_engine("sqlite:///:memory:")  # Use in-memory SQLite database for testing
    SQLModel.metadata.create_all(engine)
    return engine


@pytest.fixture()
def chunk_store() -> ChunkStore:
    return ChunkStore(
        chunk=FixedSizeChunker(chunk_size=1024),
        store=DictStore(),
    )


@pytest.fixture()
def doop_db(engine: Engine, chunk_store: ChunkStore) -> DoopMetaDB:  # noqa: ARG001 # engine is present to trigger the "engine" fixture
    return DoopMetaDB(db_url="sqlite:///:memory:", chunk_store=chunk_store)
