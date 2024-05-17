# test_doop.py
import operator
from datetime import datetime

import pytest

from snoop.doop import MetaDB


def test_add_file(doop_db: MetaDB) -> None:
    file_identifier = "file1"
    file_data = b"This is some example data that will be chunked and stored."
    file_size = len(file_data)
    now = datetime.now()

    doop_db.add_file(
        identifier=file_identifier,
        data=file_data,
        size=file_size,
        created=now,
        modified=now,
        accessed=now,
        overwrite=False,
    )

    metadata = doop_db.get_metadata(file_identifier)
    assert metadata["id"] == file_identifier
    assert metadata["size"] == file_size
    assert metadata["created"] == now
    assert metadata["modified"] == now
    assert metadata["accessed"] == now


def test_add_existing_file_without_overwrite(doop_db: MetaDB) -> None:
    file_identifier = "file1"
    file_data = b"This is some example data that will be chunked and stored."
    file_size = len(file_data)
    now = datetime.now()

    doop_db.add_file(
        identifier=file_identifier,
        data=file_data,
        size=file_size,
        created=now,
        modified=now,
        accessed=now,
        overwrite=False,
    )

    with pytest.raises(FileExistsError):
        doop_db.add_file(
            identifier=file_identifier,
            data=file_data,
            size=file_size,
            created=now,
            modified=now,
            accessed=now,
            overwrite=False,
        )


def test_add_existing_file_with_overwrite(doop_db: MetaDB) -> None:
    file_identifier = "file1"
    file_data = b"This is some example data that will be chunked and stored."
    file_size = len(file_data)
    now = datetime.now()

    doop_db.add_file(
        identifier=file_identifier,
        data=file_data,
        size=file_size,
        created=now,
        modified=now,
        accessed=now,
        overwrite=False,
    )

    new_file_data = b"This is some new data to overwrite the existing file."
    new_file_size = len(new_file_data)
    doop_db.add_file(
        identifier=file_identifier,
        data=new_file_data,
        size=new_file_size,
        created=now,
        modified=now,
        accessed=now,
        overwrite=True,
    )

    metadata = doop_db.get_metadata(file_identifier)
    assert metadata["id"] == file_identifier
    assert metadata["size"] == new_file_size

    retrieved_data = doop_db.get_data(file_identifier)
    assert retrieved_data == new_file_data


def test_get_metadata_nonexistent_file(doop_db: MetaDB) -> None:
    with pytest.raises(FileNotFoundError):
        doop_db.get_metadata("nonexistent_file")


def test_get_data_nonexistent_file(doop_db: MetaDB) -> None:
    with pytest.raises(FileNotFoundError):
        doop_db.get_data("nonexistent_file")


def test_delete_file(doop_db: MetaDB) -> None:
    file_identifier = "file1"
    file_data = b"This is some example data that will be chunked and stored."
    file_size = len(file_data)
    now = datetime.now()

    doop_db.add_file(
        identifier=file_identifier,
        data=file_data,
        size=file_size,
        created=now,
        modified=now,
        accessed=now,
        overwrite=False,
    )

    doop_db.delete(file_identifier)

    with pytest.raises(FileNotFoundError):
        doop_db.get_metadata(file_identifier)
    with pytest.raises(FileNotFoundError):
        doop_db.get_data(file_identifier)


def test_list_files(doop_db: MetaDB) -> None:
    file_identifier1 = "file1"
    file_data1 = b"This is some example data that will be chunked and stored."
    file_size1 = len(file_data1)
    now1 = datetime.now()

    file_identifier2 = "file2"
    file_data2 = b"This is some more example data for another file."
    file_size2 = len(file_data2)
    now2 = datetime.now()

    doop_db.add_file(
        identifier=file_identifier1,
        data=file_data1,
        size=file_size1,
        created=now1,
        modified=now1,
        accessed=now1,
        overwrite=False,
    )

    doop_db.add_file(
        identifier=file_identifier2,
        data=file_data2,
        size=file_size2,
        created=now2,
        modified=now2,
        accessed=now2,
        overwrite=False,
    )

    files = doop_db.list({}, operator.itemgetter("created"))
    assert len(files) == 2
    assert files[0]["id"] == file_identifier1
    assert files[1]["id"] == file_identifier2
