import hashlib

from pydantic import ConfigDict
from sqlalchemy.engine import Engine
from sqlalchemy.types import JSON
from sqlmodel import Field, Session, SQLModel, select

from .chunker import Chunker
from .key_value_store import KeyValueStore


class BlobNotFoundError(ValueError):
    """Raised when a blob with the specified identifier is not found."""


class BlobExistsError(ValueError):
    """Raised when attempting to store a blob with an identifier that already exists."""


class BlobCorruptedError(ValueError):
    """Raised when the integrity check of a blob fails."""


class Blob(SQLModel, table=True):
    """Represent a blob consisting of multiple chunks.

    Attributes
    ----------
        id: The unique identifier for the blob.
        identifier: The unique identifier string for the blob.
        hash: The hash of the blob for integrity verification.
        chunks: The list of chunk IDs that constitute the blob.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: int | None = Field(default=None, primary_key=True)
    identifier: str = Field(index=True, unique=True)
    hash: str = Field()
    # list of Chunk IDs in the order that the chunks occur in the blob
    chunks: JSON = Field(default_factory=list, sa_type=JSON)
    version: int = Field(default=1)
    meta: JSON = Field(default_factory=dict, sa_type=JSON)


class Chunk(SQLModel, table=True):
    """Represent a chunk of data that is part of a blob.

    Attributes
    ----------
        id: The unique identifier for the chunk.
        hash: The hash of the chunk for integrity verification.
        blobs: The list of blob IDs that the chunk is part of.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: int | None = Field(default=None, primary_key=True)
    hash: str = Field(index=True, unique=True)
    # set of IDs of Blobs that contain the chunk
    blobs: JSON = Field(default_factory=list, sa_type=JSON)


class BlobStore:
    """
    A store for managing blobs and their associated chunks.

    Args:
        db_engine: The SQL database engine.
        chunker: An object responsible for chunking blobs into smaller parts.
        kv_store: A key-value store for storing chunks.
    """

    def __init__(self, db_engine: Engine, chunker: Chunker, kv_store: KeyValueStore):
        self.chunker = chunker
        self.kv_store = kv_store
        self.engine = db_engine
        SQLModel.metadata.create_all(self.engine)

    def store_blob(self, identifier: str, blob_data: bytes) -> None:
        """Store a blob in the database and key-value store.

        Args:
            identifier: The unique identifier for the blob.
            blob_data: The data of the blob to be stored.

        Raises
        ------
            BlobExistsError: If a blob with the specified identifier already exists.
        """
        blob_hash = hashlib.sha256(blob_data).hexdigest()

        with Session(self.engine) as session:
            if self._blob_exists(session, identifier):
                msg = f"Blob with identifier '{identifier}' already exists."
                raise BlobExistsError(msg)

            chunk_items = self.chunker.chunk_blob(blob_data)
            self.kv_store.put_batch(chunk_items)

            chunk_keys = [hash_key for hash_key, _ in chunk_items]
            existing_chunks = self._get_existing_chunks(session, chunk_keys)
            new_chunks = [Chunk(hash=key) for key in set(chunk_keys) - existing_chunks]
            session.add_all(new_chunks)
            session.commit()

            chunk_id_map = self._get_chunk_id_map(session, chunk_keys)
            blob_chunks = [chunk_id_map[hash_key] for hash_key in chunk_keys]

            blob = Blob(identifier=identifier, hash=blob_hash, chunks=blob_chunks)
            session.add(blob)
            session.commit()

    def retrieve_blob(self, identifier: str) -> bytes:
        """Retrieve and reassemble a blob from the database and key-value store.

        Args:
            identifier: The unique identifier for the blob to be retrieved.

        Returns
        -------
            The reassembled blob data.

        Raises
        ------
            BlobNotFoundError: If the blob with the specified identifier is not found.
            BlobCorruptedError: If the integrity check of the reassembled blob fails.
        """
        with Session(self.engine) as session:
            blob = session.exec(select(Blob).where(Blob.identifier == identifier)).first()
            if not blob:
                msg = f"No blob found with identifier '{identifier}'."
                raise BlobNotFoundError(msg)

            chunk_key_map = self._get_chunk_key_map(session, blob.chunks)
            blob_data = b"".join(
                self.kv_store.get_batch([chunk_key_map[chunk_id] for chunk_id in blob.chunks])
            )

            blob_hash = hashlib.sha256(blob_data).hexdigest()
            if blob.hash != blob_hash:
                msg = "Blob integrity check failed."
                raise BlobCorruptedError(msg)

            return blob_data

    def delete_blob(self, identifier: str) -> None:
        """Delete a blob and its associated chunks from the database and key-value store.

        Args:
            identifier: The unique identifier for the blob to be deleted.

        Raises
        ------
            BlobNotFoundError: If the blob with the specified identifier is not found.
        """
        with Session(self.engine) as session:
            blob = session.exec(select(Blob).where(Blob.identifier == identifier)).first()
            if not blob:
                msg = f"No blob found with identifier '{identifier}'."
                raise BlobNotFoundError(msg)

            chunk_keys = session.exec(select(Chunk.hash).where(Chunk.id.in_(blob.chunks))).all()
            session.delete(blob)
            session.commit()

            self.kv_store.delete_batch(chunk_keys)

    @classmethod
    def _blob_exists(cls, session: Session, identifier: str) -> bool:
        """
        Check if a blob with the given identifier exists in the database.

        Args:
            session: The current database session.
            identifier: The unique identifier for the blob.

        Returns
        -------
            True if the blob exists, False otherwise.
        """
        return session.exec(select(Blob).where(Blob.identifier == identifier)).first() is not None

    @classmethod
    def _get_existing_chunks(cls, session: Session, chunk_keys: list[str]) -> set:
        """
        Retrieve existing chunks from the database based on the given hash keys.

        Args:
            session: The current database session.
            chunk_keys: The list of chunk hash keys.

        Returns
        -------
            A set of existing chunk hash keys.
        """
        return {chunk.hash for chunk in session.exec(select(Chunk).where(Chunk.hash.in_(chunk_keys))).all()}

    @classmethod
    def _get_chunk_id_map(cls, session: Session, chunk_keys: list[str]) -> dict[str, int]:
        """
        Get a mapping from chunk hash keys to chunk IDs.

        Args:
            session: The current database session.
            chunk_keys: The list of chunk hash keys.

        Returns
        -------
            A dictionary mapping chunk hash keys to chunk IDs.
        """
        chunks = session.exec(select(Chunk).where(Chunk.hash.in_(chunk_keys))).all()
        return {chunk.hash: chunk.id for chunk in chunks}

    @classmethod
    def _get_chunk_key_map(cls, session: Session, chunk_ids: list[int]) -> dict[int, str]:
        """
        Get a mapping from chunk IDs to hash keys.

        Args:
            session: The current database session.
            chunk_ids: The list of chunk hash keys.

        Returns
        -------
            A dictionary mapping chunk IDs to hash keys.
        """
        chunks = session.exec(select(Chunk).where(Chunk.id.in_(chunk_ids))).all()
        return {chunk.id: chunk.hash for chunk in chunks}
