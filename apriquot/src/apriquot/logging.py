import atexit
import datetime as dt
import json
import logging
import logging.config
from collections.abc import Generator, Iterable
from pathlib import Path

from .config import logging_config

LOG_RECORD_BUILTIN_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


def serialize(obj: object) -> dict:
    """
    Convert an object's public properties to a dictionary using a stack-based approach.

    This function serializes an object's public properties into a dictionary, handling
    various data types including basic types, collections, classes, and objects with
    special serialization needs. It avoids recursion to prevent potential recursion depth
    errors in deeply nested objects by using a stack-based approach.

    Args:
        obj (object): The object to be serialized.

    Returns
    -------
        dict: A dictionary representation of the object's public properties.

    Edge Cases:
        - Circular References: Detected using object IDs and paths, marked as "CircularReference".
        - Callable Attributes: Ignored during serialization to avoid executing methods.
        - Objects with __slots__: Serialized by extracting the values of defined slots.
        - Generators: Represented by their string representation to avoid execution.
        - Iterable objects: Converted into a list representation to capture their elements.
        - Classes: Represented by their class name and type.

    Design Choices:
        - Stack-Based Approach: Prevents recursion limit errors by using an iterative stack-based
          method instead of recursion, which is more robust for deeply nested objects.
        - Pattern Matching: Utilizes the modern pattern matching syntax (PEP 634) for cleaner
          and more readable type checks.
        - Object IDs: Uses the id() function to uniquely identify objects and detect circular or
          self-references, which ensures the integrity of the serialization process. The use of id()
          is needed because objects may not be hashable, and two different objects may have
          identical hashes.

    Examples
    --------
        >>> class Example:
        ...     def __init__(self, x, y):
        ...         self.x = x
        ...         self.y = y
        >>> obj = Example(1, [2, 3])
        >>> serialize(obj)
        {'x': 1, 'y': [2, 3], 'class': 'Example'}
    """
    stack = [(obj, None, None, ())]
    result = None
    seen = {}

    while stack:
        current_obj, parent, parent_key, path = stack.pop()

        if id(current_obj) in seen:
            if seen[id(current_obj)] == path:
                parent[parent_key] = ("SelfReference", seen[id(current_obj)])
            else:
                parent[parent_key] = ("CircularReference", seen[id(current_obj)])
            continue
        seen[id(current_obj)] = path

        match current_obj:
            case bytes() | str() | int() | float() | bool() | None:
                current_result = current_obj
            case list() as lst:
                current_result = [None] * len(lst)
                stack.extend((item, current_result, idx, (*path, idx)) for idx, item in enumerate(lst))
            case dict() as dct:
                current_result = {}
                stack.extend((v, current_result, k, (*path, k)) for k, v in dct.items() if not callable(v))
            case set() as st:
                current_result = [None] * len(st)
                stack.extend((item, current_result, idx, (*path, idx)) for idx, item in enumerate(st))
            case frozenset() as fst:
                current_result = [None] * len(fst)
                stack.extend((item, current_result, idx, (*path, idx)) for idx, item in enumerate(fst))
            case tuple() as tpl:
                current_result = [None] * len(tpl)
                stack.extend((item, current_result, idx, (*path, idx)) for idx, item in enumerate(tpl))
            case obj if hasattr(obj, "serialize") and callable(obj.serialize):
                serialized = obj.serialize()
                if isinstance(serialized, dict):
                    serialized["class"] = obj.__class__.__name__
                stack.append((serialized, parent, parent_key, path))
                continue
            case obj if hasattr(obj, "__slots__"):
                current_result = {
                    slot: getattr(obj, slot)
                    for slot in obj.__slots__
                    if hasattr(obj, slot) and not callable(getattr(obj, slot))
                }
                current_result["class"] = obj.__class__.__name__
            case cls if isinstance(cls, type):
                current_result = {"class": type(cls).__name__, "name": cls.__name__}
            case gen if isinstance(gen, Generator):
                current_result = repr(current_obj)
            case itr if isinstance(itr, Iterable):
                current_result = {"class": obj.__class__.__name__}
                iter_result = list(iter(obj))
                current_result["__iter__"] = [None] * len(iter_result)
                stack.extend(
                    (item, current_result["__iter__"], idx, (*path, idx))
                    for idx, item in enumerate(iter_result)
                )
            case obj if hasattr(obj, "__dict__"):
                current_result = {
                    k: v for k, v in obj.__dict__.items() if not k.startswith("_") and not callable(v)
                }
                current_result["class"] = obj.__class__.__name__
            case _:
                current_result = repr(current_obj)

        if parent is not None:
            parent[parent_key] = current_result
        else:
            result = current_result

    return result


class JSONFormatter(logging.Formatter):
    def __init__(
        self,
        *,
        fmt_keys: dict[str, str] | None = None,
    ):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    def format(self, record: logging.LogRecord) -> str:
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord) -> dict:
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(record.created, tz=dt.UTC).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)

        message = {
            key: (msg_val if (msg_val := always_fields.pop(val, None)) is not None else getattr(record, val))
            for key, val in self.fmt_keys.items()
        } | always_fields
        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = serialize(val)

        return message


class NonErrorFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return record.levelno <= logging.INFO


def get_logger(level="DEBUG"):
    logger = logging.getLogger(__name__)
    for handler_config in logging_config["handlers"].values():
        if (log_file := handler_config.get("filename")) is not None:
            Path(log_file).resolve().parent.mkdir(parents=True, exist_ok=True)
    logging.config.dictConfig(logging_config)
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)
    logging.basicConfig(level=level)
    return logger
