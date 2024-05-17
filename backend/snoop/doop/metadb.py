from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Self, TypeVar

from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, delete, select

from .chunk_store import ChunkStore


class Comparability(ABC):
    @abstractmethod
    def __lt__(self, other: Self) -> bool:
        """Must be defined for two objects to be Comparable."""
        ...


Comparable = TypeVar("Comparable", bound=Comparability)


class MetaDB:
    def __init__(self, db_url: str, chunk_store: ChunkStore):
        self.engine = create_engine(db_url)
        SQLModel.metadata.create_all(self.engine)
        self.chunk_store = chunk_store

    def add_file(
        self,
        identifier: str,
        data: bytes,
        size: int,
        created: datetime | None = None,
        modified: datetime | None = None,
        accessed: datetime | None = None,
        *,
        overwrite: bool = False,
    ) -> None:
        now = datetime.now(tz=UTC)

        with Session(self.engine) as session:
            # Add the file data to the chunk store and get the list of chunk hashes
            chunk_hashes = self.chunk_store.add_blob(data)
            chunks = []
            for chunk_hash in chunk_hashes:
                chunk = Chunk(chunk_hash=chunk_hash)
                session.add(chunk)
                chunks.append(chunk)

            if file := session.get(File, identifier):
                if not overwrite:
                    msg = f"A file with identifier '{identifier}' already exists."
                    raise FileExistsError(msg)
                file.size = size
                file.created = created or now
                file.modified = modified if created else now
                file.accessed = accessed if created else now
                file.chunks = chunks
            else:
                file = File(
                    id=identifier,
                    size=size,
                    created=created or now,
                    modified=modified if created else now,
                    accessed=accessed if created else now,
                    chunks=chunks,
                )
            session.add(file)
            session.commit()

    def get_metadata(self, identifier: str) -> dict:
        with Session(self.engine) as session:
            file = session.get(File, identifier)
            if file:
                return {
                    "id": file.id,
                    "size": file.size,
                    "created": file.created,
                    "modified": file.modified,
                    "accessed": file.accessed,
                }
        msg = f"No file found with identifier '{identifier}'."
        raise FileNotFoundError(msg)

    def get_data(self, identifier: str) -> bytes:
        with Session(self.engine) as session:
            file = session.get(File, identifier)
            if not file:
                msg = f"No file found with identifier '{identifier}'."
                raise FileNotFoundError(msg)

            # Retrieve and concatenate the chunks from the chunk store
            return b"".join(self.chunk_store.get_chunk(chunk.chunk_hash) for chunk in file.chunks)

    def delete(self, identifier: str) -> None:
        with Session(self.engine) as session:
            file = session.get(File, identifier)

            if not file:
                msg = f"No file found with identifier '{identifier}'."
                raise FileNotFoundError(msg)

            session.exec(delete(File).where(File.id == identifier))

            # Delete the chunks from the chunk store
            deleted_chunk_hashes = self.chunk_store.delete_blob([chunk.chunk_hash for chunk in file.chunks])
            session.exec(delete(Chunk).where(Chunk.chunk_hash in deleted_chunk_hashes))

            session.commit()

    def list(self, filters: dict, sort_key: Callable[[dict], Comparable]) -> list[dict]:
        with Session(self.engine) as session:
            statement = select(File)
            for key, value in filters.items():
                statement = statement.where(getattr(File, key) == value)

            files = session.exec(statement).all()
            files_list = [
                {
                    "id": file.id,
                    "size": file.size,
                    "created": file.created,
                    "modified": file.modified,
                    "accessed": file.accessed,
                }
                for file in files
            ]
            return sorted(files_list, key=sort_key)
