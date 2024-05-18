import hashlib
import random
import zlib
from abc import ABC, abstractmethod


class Chunker(ABC):
    @abstractmethod
    def chunk_blob(self, blob: bytes) -> list[tuple[str, bytes]]:
        """Chunk a blob into ieces and return a list of (hash_key, chunk) tuples."""

    @abstractmethod
    def hash_chunk(self, chunk: bytes) -> str:
        """Compute the hash of a chunk to act as a unique key."""

    @abstractmethod
    def compress_chunk(self, chunk: bytes) -> bytes:
        """Compress a chunk."""

    @abstractmethod
    def decompress_chunk(self, compressed_chunk: bytes) -> bytes:
        """Decompress a chunk."""


class FixedSizeChunker(Chunker):
    def __init__(self, chunk_size: int):
        super().__init__()
        self.chunk_size = chunk_size

    @staticmethod
    def compress_chunk(chunk: bytes) -> bytes:
        """Compress a chunk using zlib."""
        return zlib.compress(chunk)

    @staticmethod
    def decompress_chunk(compressed_chunk: bytes) -> bytes:
        """Decompress a chunk using zlib."""
        return zlib.decompress(compressed_chunk)

    @staticmethod
    def hash_chunk(chunk: bytes) -> str:
        """Compute the hash of a chunk with sha256."""
        return hashlib.sha256(chunk).hexdigest()

    def chunk_blob(self, blob: bytes) -> list[tuple[str, bytes]]:
        """Chunk a blob into fixed-size pieces and return a list of (hash_key, chunk) tuples."""
        chunks = [blob[i : i + self.chunk_size] for i in range(0, len(blob), self.chunk_size)]
        return [(self.hash_chunk(chunk), chunk) for chunk in chunks]


class FastCDCChunker(Chunker):
    def __init__(self, min_chunk_size: int, avg_chunk_size: int, max_chunk_size: int, mask: int):
        super().__init__()
        self.min_chunk_size = min_chunk_size
        self.avg_chunk_size = avg_chunk_size
        self.max_chunk_size = max_chunk_size
        self.mask = mask
        # TODO: consider other gear table generation parameters
        # gear table with 256 random 32-bit integers
        self.gear_table = [random.getrandbits(32) for _ in range(256)]

    @staticmethod
    def compress_chunk(chunk: bytes) -> bytes:
        """Compress a chunk using zlib."""
        return zlib.compress(chunk)

    @staticmethod
    def decompress_chunk(compressed_chunk: bytes) -> bytes:
        """Decompress a chunk using zlib."""
        return zlib.decompress(compressed_chunk)

    @staticmethod
    def hash_chunk(chunk: bytes) -> tuple[str, bytes]:
        """Compute the hash of a chunk and return the hash and the chunk."""
        hash_key = hashlib.sha256(chunk).hexdigest()
        return hash_key, chunk

    def chunk_blob(self, blob: bytes) -> list[tuple[str, bytes]]:
        """Chunk a blob based on content using FastCDC and return a list of (hash_key, chunk) tuples."""
        chunks = []
        chunk_start = 0
        gear_hash_value = 0
        i = 0

        while i < len(blob):
            gear_hash_value = ((gear_hash_value << 1) + self.gear_table[blob[i]]) & 0xFFFFFFFF

            if (i - chunk_start >= self.min_chunk_size and (gear_hash_value & self.mask) == 0) or (
                i - chunk_start >= self.max_chunk_size
            ):
                # Normalize chunk size
                end = i
                while (
                    end < len(blob)
                    and end - chunk_start < self.max_chunk_size
                    and (end - chunk_start < self.avg_chunk_size or (gear_hash_value & self.mask) != 0)
                ):
                    gear_hash_value = ((gear_hash_value << 1) + self.gear_table[blob[end]]) & 0xFFFFFFFF
                    end += 1

                chunk = blob[chunk_start:end]
                chunks.append(self.hash_chunk(chunk))
                chunk_start = end
                i = end
                gear_hash_value = 0
            else:
                i += 1

        if chunk_start < len(blob):
            chunk = blob[chunk_start:]
            chunks.append(self.hash_chunk(chunk))

        return chunks


# Example usage
def example_usage():
    fixed_chunker = FixedSizeChunker(chunk_size=32)
    fastcdc_chunker = FastCDCChunker(min_chunk_size=16, avg_chunk_size=32, max_chunk_size=64, mask=0x1F)

    blob_data = b"This is a test blob." * 20  # Example blob data

    fixed_chunks = fixed_chunker.chunk_blob(blob_data)
    content_chunks = fastcdc_chunker.chunk_blob(blob_data)

    print("Fixed Size Chunking:")
    for hash_key, chunk in fixed_chunks:
        print(f"{len(chunk)}  {hash_key[:8]}... {chunk[:16]}...")

    print("\nContent-Defined Chunking with FastCDC:")
    for hash_key, chunk in content_chunks:
        print(f"{len(chunk)}  {hash_key[:8]}... {chunk[:16]}...")


if __name__ == "__main__":
    example_usage()
