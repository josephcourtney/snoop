import pytest


def test_fixed_size_chunker(fixed_size_chunker):
    blob_data = b"This is a test blob to chunk." * 10  # Example blob data
    chunks = fixed_size_chunker.chunk_blob(blob_data)

    expected_chunks = [
        (
            fixed_size_chunker.hash_chunk(blob_data[i : i + fixed_size_chunker.chunk_size]),
            blob_data[i : i + fixed_size_chunker.chunk_size],
        )
        for i in range(0, len(blob_data), fixed_size_chunker.chunk_size)
    ]

    assert chunks == expected_chunks


def test_fast_content_defined_chunker(fast_content_defined_chunker):
    blob_data = b"This is a test blob to chunk using FastCDC."  # Example blob data
    chunks = fast_content_defined_chunker.chunk_blob(blob_data)

    # Verify that the chunks respect the min, avg, and max chunk size constraints
    assert all(
        fast_content_defined_chunker.min_chunk_size
        <= len(chunk)
        <= fast_content_defined_chunker.max_chunk_size
        for key, chunk in chunks[:-1]
    )
    # The last chunk is a special case hich is allowed to be smaller than the minimum
    assert len(chunks[-1]) <= fast_content_defined_chunker.max_chunk_size
    assert sum(len(chunk) for key, chunk in chunks) == len(blob_data)


@pytest.mark.parametrize("chunker_type", ["fixed_size_chunker", "fast_content_defined_chunker"])
def test_chunker_single_chunk(request, chunker_type):
    chunker = request.getfixturevalue(chunker_type)
    blob_data = b"1234567"  # Blob data smaller than chunk size
    chunks = chunker.chunk_blob(blob_data)
    expected_chunks = [(chunker.hash_chunk(blob_data), blob_data)]
    assert chunks == expected_chunks


@pytest.mark.parametrize("chunker_type", ["fixed_size_chunker", "fast_content_defined_chunker"])
def test_chunk_blob_length(request, chunker_type):
    chunker = request.getfixturevalue(chunker_type)
    blob = b"This is a test blob that will be chunked into smaller pieces."
    chunks = chunker.chunk_blob(blob)
    # Check that all chunks together reconstruct the original blob
    reconstructed_blob = b"".join(chunk for _, chunk in chunks)
    assert reconstructed_blob == blob


@pytest.mark.parametrize("chunker_type", ["fixed_size_chunker", "fast_content_defined_chunker"])
def test_chunk_blob_hash(request, chunker_type):
    chunker = request.getfixturevalue(chunker_type)
    blob = b"This is another test blob for hash verification."
    chunks = chunker.chunk_blob(blob)
    # Check that each chunk's hash matches the hash of the chunk
    for hash_key, chunk in chunks:
        assert hash_key == chunker.hash_chunk(chunk)


def test_fast_content_defined_chunker_avg_size(fast_content_defined_chunker):
    blob = (
        b"This is a longer blob to verify that the average chunk size is respected by the content-defined chunker."
        * 10
    )
    chunks = fast_content_defined_chunker.chunk_blob(blob)
    total_size = sum(len(chunk) for _, chunk in chunks)
    avg_chunk_size = total_size / len(chunks)
    assert avg_chunk_size >= 16
    assert avg_chunk_size <= 64


@pytest.mark.parametrize("chunker_type", ["fixed_size_chunker", "fast_content_defined_chunker"])
def test_empty_blob(request, chunker_type):
    chunker = request.getfixturevalue(chunker_type)
    blob = b""
    chunks = chunker.chunk_blob(blob)
    assert chunks == []


@pytest.mark.parametrize("chunker_type", ["fixed_size_chunker", "fast_content_defined_chunker"])
def test_single_byte_blob(request, chunker_type):
    chunker = request.getfixturevalue(chunker_type)
    blob = b"A"
    chunks = chunker.chunk_blob(blob)
    assert len(chunks) == 1
    assert chunks[0][1] == blob


@pytest.mark.parametrize("chunker_type", ["fixed_size_chunker", "fast_content_defined_chunker"])
def test_large_blob(request, chunker_type):
    chunker = request.getfixturevalue(chunker_type)
    blob = b"A" * 10000
    chunks = chunker.chunk_blob(blob)
    # Check that all chunks together reconstruct the original blob
    reconstructed_blob = b"".join(chunk for _, chunk in chunks)
    assert reconstructed_blob == blob
