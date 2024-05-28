import lzma
import zlib
from abc import ABC, abstractmethod

import brotli
import zstandard as zstd


class Compressor(ABC):
    @abstractmethod
    def compress(self, data: bytes) -> bytes:
        pass

    @abstractmethod
    def decompress(self, data: bytes) -> bytes:
        pass


class DummyCompressor(Compressor):
    """No compression, pass-through."""

    @staticmethod
    def compress(data: bytes) -> bytes:
        return data

    @staticmethod
    def decompress(data: bytes) -> bytes:
        return data


class ZstdCompressor(Compressor):
    """Compressor using Zstandard algorithm."""

    def __init__(self, dictionary: bytes | None = None):
        if dictionary:
            self.dict = zstd.ZstdCompressionDict(dictionary)
            self.compressor = zstd.ZstdCompressor(dict_data=self.dict)
            self.decompressor = zstd.ZstdDecompressor(dict_data=self.dict)
        else:
            self.compressor = zstd.ZstdCompressor()
            self.decompressor = zstd.ZstdDecompressor()

    def compress(self, data: bytes) -> bytes:
        return self.compressor.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self.decompressor.decompress(data)


class ZlibCompressor(Compressor):
    """Compressor using zlib algorithm."""

    @staticmethod
    def compress(data: bytes) -> bytes:
        return zlib.compress(data)

    @staticmethod
    def decompress(data: bytes) -> bytes:
        return zlib.decompress(data)


class LzmaCompressor(Compressor):
    """Compressor using LZMA algorithm."""

    @staticmethod
    def compress(data: bytes) -> bytes:
        return lzma.compress(data)

    @staticmethod
    def decompress(data: bytes) -> bytes:
        return lzma.decompress(data)


class BrotliCompressor(Compressor):
    """Compressor using Brotli algorithm."""

    def __init__(self, dictionary: bytes | None = None):
        self.dictionary = dictionary

    def compress(self, data: bytes) -> bytes:
        if self.dictionary:
            return brotli.compress(data, dictionary=self.dictionary)
        return brotli.compress(data)

    def decompress(self, data: bytes) -> bytes:
        if self.dictionary:
            return brotli.decompress(data, dictionary=self.dictionary)
        return brotli.decompress(data)
