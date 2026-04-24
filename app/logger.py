from __future__ import annotations

import logging
from typing import Iterable


class SecretRedactionFilter(logging.Filter):
    def __init__(self, secrets: Iterable[str] | None = None) -> None:
        super().__init__()
        self._secrets = [secret for secret in set(secrets or []) if secret]

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for secret in self._secrets:
            message = message.replace(secret, "***REDACTED***")
        record.msg = message
        record.args = ()
        return True


def configure_logging(level: str = "INFO", secrets: Iterable[str] | None = None) -> logging.Logger:
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(level.upper())

    handler = logging.StreamHandler()
    handler.setLevel(level.upper())
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    handler.addFilter(SecretRedactionFilter(secrets))
    logger.addHandler(handler)

    return logging.getLogger("trading_alert_bot")
