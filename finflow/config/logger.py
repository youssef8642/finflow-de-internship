"""Shared logging configuration for the FinFlow pipeline."""

from __future__ import annotations

import logging
import os
from typing import Optional, Union


DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _resolve_level(level: Optional[Union[str, int]]) -> int:
    """Turn a level name, level number, or None into a logging level."""
    if level is None:
        level = os.getenv("FINFLOW_LOG_LEVEL", "INFO")

    if isinstance(level, int):
        return level

    return getattr(logging, level.upper(), logging.INFO)


def configure_logging(level: Optional[Union[str, int]] = None) -> None:
    """Apply the shared format to the root logger.

    Safe to call more than once: an existing handler has its formatter
    reapplied rather than a second one being attached, which is what would
    otherwise duplicate every line once several modules are imported.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(_resolve_level(level))

    if not root_logger.handlers:
        root_logger.addHandler(logging.StreamHandler())

    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT)
    for handler in root_logger.handlers:
        handler.setFormatter(formatter)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger, configuring logging on first use."""
    configure_logging()
    return logging.getLogger(name or "finflow")


configure_logging()
logger = get_logger("finflow")
