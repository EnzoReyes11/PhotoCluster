"""Logging configuration for the PhotoCluster application.

This module provides centralized logging configuration for the application.
It handles both file and console logging with consistent formatting and
automatic log file naming based on the calling module.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name.

    Args:
        name: The name for the logger, typically __name__ of the calling module

    Returns:
        A configured logger instance

    """
    return logging.getLogger(name)


def setup_logging(
    log_file_name: str | None,
    log_level: int = logging.INFO,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
) -> None:
    """Configure logging for the application.

    This function sets up logging with both console and file handlers. The log file
    will be named after the calling module unless a specific name is provided.

    Args:
        log_file_name: Optional name for the log file. If None, uses the current file's name.
        log_level: The logging level to use (default: logging.INFO)
        log_format: The format string for log messages

    Raises:
        OSError: If there are permission issues creating the log file

    Example:
        >>> from logger import setup_logging, get_logger
        >>> setup_logging()  # Uses calling module name for log file
        >>> logger = get_logger(__name__)
        >>> logger.info("Application started")

    """
    if log_file_name is None:
        # Get the name of the calling file without extension
        import inspect

        frame = inspect.stack()[1]
        module = inspect.getmodule(frame[0])
        log_file_name = Path(module.__file__).stem if module is not None else "app"

    try:
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f"{log_file_name}.log"),
            ],
        )
    except OSError as e:
        # If we can't create the log file, at least set up console logging
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[logging.StreamHandler(sys.stdout)],
        )
        logger.warning("Could not create log file: %s", e)
