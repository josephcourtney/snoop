from enum import auto

import pytest
from apriquot.ordered_enum import OrderedEnum


class LogLevel(OrderedEnum):
    CRITICAL = auto()
    ERROR = auto()
    WARNING = auto()
    INFO = auto()
    DEBUG = auto()


def test_case_insensitive_string_comparison():
    """Test case-insensitive string comparison for enum members."""
    assert LogLevel.INFO == "info"
    assert LogLevel.INFO == "INFO"
    assert LogLevel.INFO == "Info"
    assert LogLevel.WARNING != "Info"


def test_member_comparison():
    """Test comparison between enum members."""
    assert LogLevel.INFO < LogLevel.DEBUG
    assert LogLevel.WARNING > LogLevel.INFO


def test_invalid_value():
    """Test that an invalid value raises the appropriate exception."""
    with pytest.raises(ValueError, match="'notalevel' is not a valid LogLevel"):
        LogLevel("notalevel")


def test_hash():
    """Test the hash value of an enum member."""
    assert hash(LogLevel.INFO) == hash("info")


def test_equality_with_other_enum_members():
    """Test equality comparison between enum members."""
    assert LogLevel.INFO == LogLevel.INFO
    assert LogLevel.INFO != LogLevel.DEBUG


def test_string_representation():
    """Test the string representation of an enum member."""
    assert str(LogLevel.INFO) == "LogLevel.INFO"
    assert repr(LogLevel.INFO) == "<LogLevel.INFO: info>"


def test_from_value():
    """Test creating an enum member from a value."""
    assert LogLevel("info") == LogLevel.INFO
    with pytest.raises(ValueError, match="'notalevel' is not a valid LogLevel"):
        LogLevel("notalevel")


def test_list_values():
    """Test listing all values of the enum."""
    assert LogLevel.values() == ["critical", "error", "warning", "info", "debug"]


def test_list_members():
    """Test listing all members of the enum."""
    assert LogLevel.members() == [
        LogLevel.CRITICAL,
        LogLevel.ERROR,
        LogLevel.WARNING,
        LogLevel.INFO,
        LogLevel.DEBUG,
    ]
