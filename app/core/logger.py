from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

from app.core.settings import settings


_LOGGER_INITIALIZED = False


class ContextFormatter(logging.Formatter):
    """
    Safe formatter that supports optional extra fields without crashing.

    Supported extra fields:
    - cycle_id
    - symbol
    - component
    """

    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "cycle_id"):
            record.cycle_id = "-"
        if not hasattr(record, "symbol"):
            record.symbol = "-"
        if not hasattr(record, "component"):
            record.component = "-"
        return super().format(record)


def _resolve_log_level(level_name: str) -> int:
    level = getattr(logging, level_name.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Invalid log level: {level_name!r}")
    return level


def _build_stream_handler(level: int) -> logging.Handler:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(
        ContextFormatter(
            fmt=(
                "%(asctime)s | %(levelname)-8s | %(name)s | "
                "component=%(component)s | cycle=%(cycle_id)s | symbol=%(symbol)s | "
                "%(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    return handler


def _build_file_handler(level: int, log_file_path: Path) -> logging.Handler:
    handler = logging.FileHandler(log_file_path, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(
        ContextFormatter(
            fmt=(
                "%(asctime)s | %(levelname)-8s | %(name)s | "
                "component=%(component)s | cycle=%(cycle_id)s | symbol=%(symbol)s | "
                "%(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    return handler


def setup_logging(force: bool = False) -> None:
    """
    Configure root logger for the whole application.

    Features:
    - stdout logging for cloud platforms
    - file logging into logs/app.log
    - safe repeated initialization
    - consistent formatting with context fields
    """
    global _LOGGER_INITIALIZED

    if _LOGGER_INITIALIZED and not force:
        return

    log_level = _resolve_log_level(settings.log_level)

    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    log_file_path = settings.logs_dir / "app.log"

    root_logger = logging.getLogger()

    if force:
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            try:
                handler.close()
            except Exception:
                pass
    else:
        if root_logger.handlers:
            _LOGGER_INITIALIZED = True
            return

    root_logger.setLevel(log_level)

    stream_handler = _build_stream_handler(log_level)
    file_handler = _build_file_handler(log_level, log_file_path)

    root_logger.addHandler(stream_handler)
    root_logger.addHandler(file_handler)

    logging.captureWarnings(True)

    _LOGGER_INITIALIZED = True

    logger = logging.getLogger("app.bootstrap")
    logger.info(
        "Logging initialized.",
        extra={
            "component": "logger",
            "cycle_id": "-",
            "symbol": "-",
        },
    )
    logger.info(
        f"Environment={settings.app_env} | log_level={settings.log_level} | logs_dir={settings.logs_dir}",
        extra={
            "component": "logger",
            "cycle_id": "-",
            "symbol": "-",
        },
    )


def get_logger(name: str, component: Optional[str] = None) -> logging.LoggerAdapter:
    """
    Returns a LoggerAdapter with default contextual fields.

    Example:
        logger = get_logger(__name__, component="worker")
        logger.info("Cycle started")

        logger.info(
            "Analyzing instrument",
            extra={"symbol": "XAUUSD", "cycle_id": "2026-03-31T07:00:00Z"}
        )
    """
    base_logger = logging.getLogger(name)
    return logging.LoggerAdapter(
        base_logger,
        {
            "component": component or name,
            "cycle_id": "-",
            "symbol": "-",
        },
    )


def bind_logger(
    logger: logging.LoggerAdapter,
    *,
    component: Optional[str] = None,
    cycle_id: Optional[str] = None,
    symbol: Optional[str] = None,
) -> logging.LoggerAdapter:
    """
    Returns a new LoggerAdapter with updated contextual fields.

    Example:
        base_logger = get_logger(__name__, component="runner")
        cycle_logger = bind_logger(base_logger, cycle_id="2026-03-31T07:00:00Z")
        xau_logger = bind_logger(cycle_logger, symbol="XAUUSD")
    """
    extra = dict(getattr(logger, "extra", {}) or {})

    if component is not None:
        extra["component"] = component
    if cycle_id is not None:
        extra["cycle_id"] = cycle_id
    if symbol is not None:
        extra["symbol"] = symbol

    return logging.LoggerAdapter(logger.logger, extra)


def log_exception(
    logger: logging.LoggerAdapter,
    message: str,
    *,
    component: Optional[str] = None,
    cycle_id: Optional[str] = None,
    symbol: Optional[str] = None,
) -> None:
    """
    Convenience wrapper for structured exception logging.
    """
    contextual_logger = bind_logger(
        logger,
        component=component,
        cycle_id=cycle_id,
        symbol=symbol,
    )
    contextual_logger.exception(message)