from pydantic import ConfigDict
from sqlalchemy.types import JSON
from sqlmodel import Field, Relationship, SQLModel


class BlobNotFoundError(ValueError):
    """Raised when a blob with the specified identifier is not found."""


class BlobExistsError(ValueError):
    """Raised when attempting to store a blob with an identifier that already exists."""


class BlobCorruptedError(ValueError):
    """Raised when the integrity check of a blob fails."""


class BlobChunkLink(SQLModel, table=True):
    """Represent the inclusion and position of a chunk in blob."""

    blob_id: int | None = Field(default=None, foreign_key="blob.id", primary_key=True)
    chunk_id: int | None = Field(default=None, foreign_key="chunk.id", primary_key=True)
    order: int = Field(primary_key=True)

    blob: "Blob" = Relationship(back_populates="chunk_links")
    chunk: "Chunk" = Relationship(back_populates="blob_links")


class Chunk(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    key: bytes

    blob_links: list[BlobChunkLink] = Relationship(back_populates="chunk")


class Blob(SQLModel, table=True):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: int | None = Field(default=None, primary_key=True)
    identifier: bytes = Field(unique=True)
    hash: bytes
    version: int = Field(default=0)
    meta: JSON = Field(default_factory=dict, sa_type=JSON)
    chunk_links: list[BlobChunkLink] = Relationship(back_populates="blob")
