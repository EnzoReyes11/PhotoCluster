"""Logging configuration module.

This module provides centralized logging configuration for applications.
It handles both file and console logging with consistent formatting.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Logger for this module itself, can be used for messages about the logging setup.
module_logger = logging.getLogger(__name__)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the specified name.

    Args:
        name: The name for the logger, typically __name__ of the calling module.

    Returns:
        A configured logger instance.

    """
    return logging.getLogger(name)


def setup_logging(
    log_file_name_source: str | None = None,
    log_level: int = logging.INFO,
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    log_directory: str | Path | None = None,
) -> None:
    """Configure logging for the application.

    This function sets up logging with both console and file handlers.
    It's recommended to call this function only once at the application's start.

    Args:
        log_file_name_source: Optional base name for the log file (e.g., "my_app").
                       If None, "application.log" will be used.
                       The ".log" extension will be appended if not present.
        log_level: The logging level to use (default: logging.INFO).
        log_format: The format string for log messages.
        log_directory: Optional directory to store log files. If None,
                       logs are created in the current working directory.

    Example:
        >>> # In your main application file (e.g., main.py)
        >>> # from custom_logger import setup_logging, get_logger
        >>>
        >>> # Option 1: Provide a specific log file name and level
        >>> # setup_logging(log_file_name="photo_analyzer", log_level=logging.DEBUG)
        >>> # main_app_logger = get_logger("my_photo_app")
        >>> # main_app_logger.info("Application started with photo_analyzer.log")

    """
    if log_file_name_source is None:
        base_stem = "application"
    else:
        # Get the stem of the provided name (e.g., "script" from "script.py")
        base_stem = Path(log_file_name_source).stem

    # Always append .log to the derived stem
    final_log_filename = f"{base_stem}.log"

    if log_directory:
        log_dir_path = Path(log_directory)
        log_dir_path.mkdir(parents=True, exist_ok=True)
        final_log_file_path = log_dir_path / final_log_filename
    else:
        final_log_file_path = Path(final_log_filename)

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
    ]

    log_message_suffix_args: list[str | Path] = []
    try:
        file_handler = logging.FileHandler(final_log_file_path)
        handlers.append(file_handler)
        log_message_suffix = "Log file: %s, Level: %s"
        log_message_suffix_args.extend(
            [str(final_log_file_path), logging.getLevelName(log_level)],
        )

    except OSError as e:
        temp_console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(log_format)
        temp_console_handler.setFormatter(formatter)

        if not module_logger.hasHandlers() or not any(
            isinstance(h, logging.StreamHandler) for h in module_logger.handlers
        ):
            module_logger.addHandler(temp_console_handler)
            # Set level explicitly for module_logger if it's not configured
            if module_logger.level == logging.NOTSET:
                module_logger.setLevel(logging.WARNING)

        module_logger.warning(
            "Could not create log file '%s': %s. Logging to console only.",
            final_log_file_path,
            e,
        )
        if (
            temp_console_handler in module_logger.handlers
        ):  # Remove only if added by this block
            module_logger.removeHandler(temp_console_handler)

        log_message_suffix = "Console only, Level: %s"
        log_message_suffix_args.append(logging.getLevelName(log_level))

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
    )

    full_log_message = f"Logging configured. {log_message_suffix}"
    module_logger.info(
        full_log_message,
        *log_message_suffix_args,
    )
