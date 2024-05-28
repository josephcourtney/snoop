logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "[%(levelname)s|%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
        "json": {
            "()": "weasel.logging.JSONFormatter",
            "fmt_keys": {
                "level": "levelname",
                "message": "message",
                "timestamp": "timestamp",
                "logger": "name",
                "module": "module",
                "function": "funcName",
                "line": "lineno",
                "thread_name": "threadName",
            },
        },
    },
    "handlers": {
        "stderr": {
            "class": "logging.StreamHandler",
            "level": "WARNING",
            "formatter": "simple",
            "stream": "ext://sys.stderr",
        },
        "file_json": {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "json",
            "filename": "logs/weasel.log.jsonl",
            "maxBytes": 10000,
            "backupCount": 3,
        },
        "queue_handler": {
            "class": "logging.handlers.QueueHandler",
            "handlers": ["stderr", "file_json"],
            "respect_handler_level": True,
        },
    },
    "loggers": {"root": {"level": "DEBUG", "handlers": ["queue_handler"]}},
}
