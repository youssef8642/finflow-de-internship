"""Shared logging configuration for the Finflow workspace."""

from __future__ import annotations

import logging
import os
from typing import Optional, Union


DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOGGING_CONFIGURED = False


def _resolve_level(level: Optional[Union[str, int]]) -> int:
	if level is None:
		level = os.getenv("FINFLOW_LOG_LEVEL", "INFO")

	if isinstance(level, int):
		return level

	return getattr(logging, level.upper(), logging.INFO)


def configure_logging(level: Optional[Union[str, int]] = None) -> None:
	"""Configure the process-wide logging format once."""
	global _LOGGING_CONFIGURED

	if _LOGGING_CONFIGURED:
		return

	resolved_level = _resolve_level(level)
	root_logger = logging.getLogger()
	root_logger.setLevel(resolved_level)

	formatter = logging.Formatter(
		DEFAULT_LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT
	)

	if root_logger.handlers:
		for handler in root_logger.handlers:
			handler.setFormatter(formatter)
	else:
		handler = logging.StreamHandler()
		handler.setFormatter(formatter)
		root_logger.addHandler(handler)

	_LOGGING_CONFIGURED = True


def get_logger(name: Optional[str] = None) -> logging.Logger:
	"""Return a logger after ensuring logging is configured."""
	configure_logging()
	return logging.getLogger(name or "finflow")


configure_logging()
logger = get_logger("finflow")
