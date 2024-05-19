import hashlib
import random
from abc import ABC, abstractmethod


class Chunker(ABC):
    @abstractmethod
    def chunk_blob(self, blob: bytes) -> list[tuple[str, bytes]]:
        """Chunk a blob into ieces and return a list of (hash_key, chunk) tuples."""

    @abstractmethod
    def hash_chunk(self, chunk: bytes) -> str:
        """Compute the hash of a chunk to act as a unique key."""


class FixedSizeChunker(Chunker):
    def __init__(self, chunk_size: int):
        super().__init__()
        self.chunk_size = chunk_size

    @staticmethod
    def hash_chunk(chunk: bytes) -> str:
        """Compute the hash of a chunk with sha256."""
        return hashlib.sha256(chunk).hexdigest()

    def chunk_blob(self, blob: bytes) -> list[tuple[str, bytes]]:
        """Chunk a blob into fixed-size pieces and return a list of (key, chunk) tuples."""
        chunks = [blob[i : i + self.chunk_size] for i in range(0, len(blob), self.chunk_size)]
        return [(self.hash_chunk(chunk), chunk) for chunk in chunks]


class AdaptiveChunker(Chunker):
    """Chunker that switches between other Chunkers based on data characteristics."""

    def __init__(self, strategies: list[Chunker]):
        self.strategies = strategies

    def select_strategy(self, blob: bytes, blob_metadata: dict | None = None) -> Chunker:
        raise NotImplementedError

    def chunk_blob(self, blob: bytes, blob_metadata: dict | None = None) -> list[tuple[str, bytes]]:
        # Choose a strategy based on blob size or content type
        strategy = self.select_strategy(blob, blob_metadata)
        strategy = self.strategies[0]  # Simplified example
        return strategy.chunk_blob(blob)

    @staticmethod
    def hash_chunk(chunk: bytes) -> str:
        """Compute the hash of a chunk with sha256."""
        return hashlib.sha256(chunk).hexdigest()


class FastContentDefinedChunker(Chunker):
    """Chunker that uses FastCDC for content-defined chunking."""

    def __init__(self, min_chunk_size: int, avg_chunk_size: int, max_chunk_size: int, mask: int):
        self.min_chunk_size = min_chunk_size
        self.avg_chunk_size = avg_chunk_size
        self.max_chunk_size = max_chunk_size
        self.mask = mask
        self.gear_table = [random.getrandbits(32) for _ in range(256)]

    @staticmethod
    def hash_chunk(chunk: bytes) -> str:
        """Compute the hash of a chunk with sha256."""
        return hashlib.sha256(chunk).hexdigest()

    def chunk_blob(self, blob: bytes) -> list[tuple[str, bytes]]:
        """Chunk a blob based on content using FastCDC and return a list of (hash_key, chunk) tuples."""
        chunks = []
        chunk_start = 0
        gear_hash_value = 0
        length = len(blob)

        for i in range(length):
            gear_hash_value = ((gear_hash_value << 1) + self.gear_table[blob[i]]) & 0xFFFFFFFF

            if i - chunk_start >= self.min_chunk_size and (
                (gear_hash_value & self.mask) == 0 or i - chunk_start >= self.max_chunk_size
            ):
                end = min(i, chunk_start + self.max_chunk_size)
                chunk = blob[chunk_start:end]
                chunks.append((self.hash_chunk(chunk), chunk))
                chunk_start = end
                gear_hash_value = 0

        if chunk_start < length:
            chunk = blob[chunk_start:]
            chunks.append((self.hash_chunk(chunk), chunk))

        return chunks
