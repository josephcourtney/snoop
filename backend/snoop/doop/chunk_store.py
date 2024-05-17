from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable

from sqlmodel import Field, Relationship, Session, SQLModel, create_engine


class Chunker(ABC):
    @abstractmethod
    def __call__(self, data: bytes) -> Generator[tuple[bytes, bytes]]:
        """Break a blob of data into a series of chunks and return a list of (chunk, hash) tuples."""
        ...

    @abstractmethod
    def hash(self, chunk: bytes) -> bytes:
        """Calculate the hash of a chunk."""
        ...


class Store(ABC):
    @abstractmethod
    def put(self, chunks_with_keys: Iterable[tuple[bytes, bytes]]) -> None:
        """Take an Iterable of (chunk, hash) tuples and upsert their records."""
        ...

    @abstractmethod
    def get(self, keys: Iterable[bytes]) -> Iterable[bytes]:
        """Return the chunks corresponding to an Iterable of keys."""
        ...

    @abstractmethod
    def delete(self, keys: Iterable[bytes]) -> Generator[bytes]:
        """Delete one "copy" of the chunk.

        Decrement the reference counters to chunks corresponding to an Iterable of keys; If the reference
        counter for a chunk drops to zero, remove that chunk from the store and yeild its key.
        """
        ...


class BlobChunkLink(SQLModel, table=True):
    blob_id: int | None = Field(foreign_key="blob.id", primary_key=True)
    chunk_id: int | None = Field(foreign_key="chunk.id", primary_key=True)


class Blob(SQLModel, table=True):
    id: str = Field(primary_key=True)
    chunks: list["Chunk"] = Relationship(back_populates="blobs", link_model=BlobChunkLink)


class Chunk(SQLModel, table=True):
    id: bytes = Field(primary_key=True)
    key: bytes


class ChunkStore:
    def __init__(self, db_url: str, chunk: Chunker, store: Store):
        self.chunk = chunk()
        self.store = store()

        self.engine = create_engine(db_url)
        SQLModel.metadata.create_all(self.engine)

    def put(self, identifier: bytes, data: bytes, *, overwrite: bool = False) -> None:
        chunks = []
        with Session(self.engine) as session:
            for key, _ in self.chunk(data):
                chunk = Chunk(key)
                session.add(chunk)
                chunks.append(chunk)

            if blob := session.get(Blob, identifier):
                if not overwrite:
                    msg = f"A blob with identifier '{identifier}' already exists."
                    raise FileExistsError(msg)
            else:
                blob = Blob(identifier, chunks)
            session.add(blob)
            session.commit()

    def get(self, identifiers: Iterable[bytes]) -> Generator[Generator[bytes]]:
        with Session(self.engine) as session:
            for identifier in identifiers:
                if not (blob := session.get(Blob, identifier)):
                    msg = f"A blob with identifier '{identifier}' does not exist."
                    raise FileNotFoundError(msg)
                yield self.store.get(chunk.key for chunk in blob.chunks)

    def delete(self, keys: Iterable[bytes]) -> None:
        with Session(self.engine) as session:
            for deleted_key in self.store.delete(keys):
                session.delete(Chunk, deleted_key)


class FixedSizeChunker(Chunker):
    def __init__(self, chunk_size: int):
        self.chunk_size = chunk_size

    def __call__(self, data: bytes) -> list[bytes]:
        return [data[i : i + self.chunk_size] for i in range(0, len(data), self.chunk_size)]


class DictStore(Store):
    def __init__(self):
        self.store = {}
        self.ref_count = {}

    def put(self, key: str, data: bytes) -> None:
        self.store[key] = data
        self.ref_count[key] = self.ref_count.get(key, 0) + 1

    def get(self, key: str) -> bytes:
        return self.store.get(key, b"")

    def delete(self, key: str) -> None:
        if key in self.ref_count:
            del self.store[key]
            del self.ref_count[key]

    def increment_ref_count(self, key: str) -> None:
        self.ref_count[key] = self.ref_count.get(key, 0) + 1

    def decrement_ref_count(self, key: str) -> None | str:
        if key in self.ref_count:
            self.ref_count[key] -= 1
            if self.ref_count[key] == 0:
                self.delete(key)
                return True
            return None
        return None


# import fastcdc
# class FastContentDefinedChunker(Chunker):
#     def __init__(self, min_size: int, avg_size: int, max_size: int):
#         self.min_size = min_size
#         self.avg_size = avg_size
#         self.max_size = max_size
#
#     def __call__(self, data: bytes) -> list[bytes]:
#         return [chunk for chunk in fastcdc.split(data, self.min_size, self.avg_size, self.max_size)]


# import redis
# class RedisStore(Store):
#     def __init__(self, redis_host='localhost', redis_port=6379):
#         self.client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
#
#     def put(self, key: str, data: bytes) -> None:
#         self.client.set(key, data)
#         self.increment_ref_count(key)
#
#     def get(self, key: str) -> bytes:
#         return self.client.get(key)
#
#     def delete(self, key: str) -> None:
#         self.client.delete(key)
#         self.client.delete(f"ref_{key}")
#
#     def increment_ref_count(self, key: str) -> None:
#         self.client.incr(f"ref_{key}")
#
#     def decrement_ref_count(self, key: str) -> None:
#         if self.client.decr(f"ref_{key}") <= 0:
#             self.delete(key)
#
#
