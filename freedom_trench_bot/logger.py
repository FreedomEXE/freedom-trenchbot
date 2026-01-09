from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

STANDARD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            data["exc_info"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key in STANDARD_ATTRS:
                continue
            data[key] = value
        return json.dumps(data, ensure_ascii=True)


def setup_logging(level: str) -> logging.Logger:
    logger = logging.getLogger("freedom_trench_bot")
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.handlers = [handler]
    logger.propagate = False

    logging.getLogger("telegram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    return logger
