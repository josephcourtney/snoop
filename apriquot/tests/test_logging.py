# ruff: noqa: S101

import pytest
from apriquot.logging import serialize


# Fixtures
@pytest.fixture()
def complex_obj():
    """Return an instance of `ComplexObj` class."""

    class ComplexObj:
        def __init__(self):
            self._private = 1
            self.public = 2

    return ComplexObj()


@pytest.fixture()
def inherited_obj():
    """Return an instance of `DerivedClass` class, which inherits from `BaseClass`."""

    class BaseClass:
        def __init__(self):
            self.base = 1

    class DerivedClass(BaseClass):
        def __init__(self):
            super().__init__()
            self.derived = 2

    return DerivedClass()


@pytest.fixture()
def object_with_serialize_method():
    """Return an instance of `MyClass` class with a `serialize` static method."""

    class MyClass:
        @staticmethod
        def serialize():
            return {"x": 1, "y": 2}

    return MyClass()


@pytest.fixture()
def object_with_methods():
    """Return an instance of `MyClass` class with an additional method."""

    class MyClass:
        def __init__(self):
            self.a = 1

        def my_method(self):
            pass

    return MyClass()


@pytest.fixture()
def object_with_slots():
    """Return an instance of `MyClass` class with `__slots__` defined."""

    class MyClass:
        __slots__ = ["a", "b"]

        def __init__(self):
            self.a = 1
            self.b = 2

    return MyClass()


@pytest.fixture()
def circular_reference_obj():
    """Return an object with a circular reference."""
    obj = {}
    obj["self"] = obj
    return obj


@pytest.fixture()
def circular_reference_objects():
    obj1 = {}
    obj1["self"] = obj1

    b = {}
    a = {"b": b}
    b["a"] = a

    b_complex = {}
    c_complex = {}
    a_complex = {"b_in_a": b_complex}
    b_complex["c_in_b"] = c_complex
    c_complex["b_in_c"] = b_complex

    return obj1, a, a_complex


# Parameterized Tests for Happy Path
@pytest.mark.parametrize(
    ("input_obj", "expected_output"),
    [
        ({"a": 1, "b": 2}, {"a": 1, "b": 2}),
        ([1, 2, 3], [1, 2, 3]),
        ((1, 2, 3), [1, 2, 3]),
        ({"a": [1, {"b": 2}]}, {"a": [1, {"b": 2}]}),
        ("string", "string"),
        (123, 123),
        (12.3, 12.3),
        (True, True),
        (None, None),
        (b"bytes", b"bytes"),
        ({"a": 1, "b": lambda x: x}, {"a": 1}),
        (type("TestObj", (), {"serialize": lambda _self: {"a": 1}})(), {"a": 1, "class": "TestObj"}),
    ],
    ids=[
        "dict_simple",
        "list_simple",
        "tuple_simple",
        "nested_structure",
        "string_value",
        "int_value",
        "float_value",
        "bool_true",
        "none_value",
        "bytes_value",
        "dict_with_function",
        "object_with_serialize_method",
    ],
)
def test_serialize_happy_path(input_obj, expected_output):
    """Tests the `serialize` function with various input objects and expected output."""
    assert serialize(input_obj) == expected_output


# Parameterized Tests for Edge Cases
@pytest.mark.parametrize(
    ("input_obj", "expected_output"),
    [
        ([], []),
        ({}, {}),
        (type("EmptyObj", (), {})(), {"class": "EmptyObj"}),
    ],
    ids=["empty_list", "empty_dict", "empty_object"],
)
def test_serialize_edge_cases(input_obj, expected_output):
    """Tests the `serialize` function with empty lists, dictionaries, and objects."""
    assert serialize(input_obj) == expected_output


def test_serialize_complex_obj(complex_obj):
    """Tests the `serialize` function with an instance of `ComplexObj` class."""
    expected_output = {"public": 2, "class": "ComplexObj"}
    assert serialize(complex_obj) == expected_output


# Parameterized Tests for Additional Robustness
@pytest.mark.parametrize(
    ("fixture_name", "expected_output"),
    [
        ("complex_obj", {"public": 2, "class": "ComplexObj"}),
        ("inherited_obj", {"base": 1, "derived": 2, "class": "DerivedClass"}),
        ("object_with_serialize_method", {"x": 1, "y": 2, "class": "MyClass"}),
        ("object_with_methods", {"a": 1, "class": "MyClass"}),
        ("object_with_slots", {"a": 1, "b": 2, "class": "MyClass"}),
    ],
    ids=[
        "complex_obj",
        "inherited_class",
        "object_with_serialize_method",
        "object_with_methods",
        "object_with_slots",
    ],
)
def test_serialize_additional_robustness(fixture_name, expected_output, request):
    """Tests the `serialize` function with additional fixtures."""
    input_obj = request.getfixturevalue(fixture_name)
    assert serialize(input_obj) == expected_output


@pytest.mark.parametrize(
    ("input_obj", "expected_output"),
    [
        ([], []),
        ({}, {}),
        (set(), []),
        ((), []),
        (frozenset([1, 2, 3]), [1, 2, 3]),
        (list(range(1000)), list(range(1000))),
        ("こんにちは", "こんにちは"),
        ("😀", "😀"),
    ],
    ids=[
        "empty_list",
        "empty_dict",
        "empty_set",
        "empty_tuple",
        "frozenset",
        "large_list",
        "unicode_string",
        "emoji_string",
    ],
)
def test_serialize_various_structures(input_obj, expected_output):
    """Tests the `serialize` function with various input structures."""
    assert serialize(input_obj) == expected_output


# Individual Tests for Special Cases
def test_serialize_generator():
    """Tests the `serialize` function with a generator object."""

    def my_gen():
        i = 0
        while True:
            yield i
            i += 1

    assert isinstance(serialize(my_gen()), str)


def test_serialize_iterable():
    """Tests the `serialize` function with an iterable object."""

    class MyIterable:
        def __iter__(self):
            return iter([1, 2, 3])

    obj = MyIterable()
    assert serialize(obj) == {"class": "MyIterable", "__iter__": [1, 2, 3]}


def test_serialize_class():
    """Tests the `serialize` function with a class object."""

    class MyClass:
        pass

    assert serialize(MyClass) == {"class": "type", "name": "MyClass"}


def test_serialize_circular_reference(circular_reference_objects):
    """Tests the `serialize` function with objects containing circular references."""
    obj1, a, a_complex = circular_reference_objects

    result = serialize(obj1)
    assert result == {"self": ("CircularReference", ())}

    result = serialize(a)
    assert result == {"b": {"a": ("CircularReference", ())}}

    result = serialize(a_complex)
    assert result == {"b_in_a": {"c_in_b": {"b_in_c": ("CircularReference", ("b_in_a",))}}}
