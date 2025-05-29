"""Extracts EXIF data from a directory tree and stores it in MongoDB.

Uses ExifTool https://exiftool.org/, which needs to be installed in the system.
The records are stored normalized, without the namespace ExitTool uses.

Step 0.
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from exiftool import ExifToolHelper

from db import get_mongodb_connection
from logger import get_logger, setup_logging

logger = get_logger(__name__)


def read_all_media_files(directory: Path, unsupported_files_log: Path) -> list[str]:
    """Read all supported image and video files from a directory tree.

    Args:
        directory: Root directory to start searching from
        unsupported_files_log: Path to log file for unsupported extensions

    Returns:
        List of absolute file paths for supported media files

    """
    file_paths: list[str] = []
    unsupported_extensions: set[str] = set()
    supported_extensions = {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".tiff",
        ".webp",
        ".mov",
        ".heic",
        ".mp4",
    }

    def read_all_files_rec(current_dir: Path) -> None:
        """Recursively read files from directory.

        Args:
            current_dir: Current directory to process

        """
        for item in current_dir.iterdir():
            if item.is_dir():
                read_all_files_rec(item)
            else:
                ext = item.suffix.lower()
                if ext in supported_extensions:
                    file_paths.append(str(item.absolute()))
                else:
                    unsupported_extensions.add(ext)

    # Start recursive file reading
    read_all_files_rec(directory)

    # Log unsupported extensions
    if unsupported_extensions:
        timestamp = datetime.now(timezone.utc).isoformat()
        with Path.open(unsupported_files_log, "a") as log_file:
            log_file.write(
                f"{timestamp}: {', '.join(sorted(unsupported_extensions))}\n",
            )

    return file_paths


def normalize_exiftool_data(metadata: dict[str, any]) -> dict[str, any]:
    """Normalize ExifTool metadata by removing prefixes from all fields.

    Args:
        metadata: Raw ExifTool metadata dictionary

    Returns:
        Normalized metadata dictionary with all fields flattened and prefixes removed

    """
    normalized = {}
    namespace_separator_parts = 2

    for key, value in metadata.items():
        # Split the key by first colon
        parts = key.split(":", 1)
        new_key = parts[1] if len(parts) == namespace_separator_parts else key
        normalized[new_key] = value

    return normalized


def _get_validated_mongo_database_env() -> str:
    """Get and validate the MONGO_DATABASE environment variable."""
    database = os.getenv("MONGO_DATABASE")
    if not database:
        msg = "MONGO_DATABASE environment variable is required"
        raise ValueError(msg)
    return database


def _get_validated_source_dir_env() -> Path:
    """Get and validate the SOURCE_IMAGES_DIR_PATH environment variable."""
    source_dir_str = os.getenv("SOURCE_IMAGES_DIR_PATH")
    if not source_dir_str:
        msg = "SOURCE_IMAGES_DIR_PATH environment variable is required"
        raise ValueError(msg)

    source_dir = Path(source_dir_str)
    if not source_dir.is_dir():
        # This check is more about file system state than missing env var.
        # Raising FileNotFoundError might be more appropriate.
        # Sticking to ValueError for env var issues.
        msg = (
            f"SOURCE_IMAGES_DIR_PATH ('{source_dir}') does not exist or is not a"
            " directory."
        ) # Shortened line
        raise ValueError(
            msg,
        )
    return source_dir


def main() -> None:
    """Extract EXIF data from supported media and store metadata in MongoDB."""
    try:
        # Setup logging
        setup_logging(__file__, log_directory="logs")

        # Load environment variables
        load_dotenv()

        # Validate environment variables by calling new helper functions
        # The try-except block here demonstrates catching errors from helpers.
        # The main function already has a general try-except, so this
        # specific block could be removed if only top-level handling is desired.
        try:
            _get_validated_mongo_database_env()
            source_dir = _get_validated_source_dir_env()
        except ValueError:
            logger.exception("Configuration error")
            # Re-raise to be caught by the main try-except block,
            # or handle more specifically if needed.
            raise

        unsupported_files_log = Path(
            os.getenv("UNSUPPORTED_FILES_LOG", "unsupported_files.log"),
        )

        # Connect to MongoDB
        client, collection = get_mongodb_connection()

        try:
            # Get all image files
            file_paths = read_all_media_files(source_dir, unsupported_files_log)
            logger.info("Found %d image files to process", len(file_paths))

            # Process files in batches to avoid memory issues
            batch_size = 1000
            for i in range(0, len(file_paths), batch_size):
                batch = file_paths[i : i + batch_size]

                logger.info(
                    "Processing batch %d of %d",
                    i // batch_size + 1,
                    (len(file_paths) + batch_size - 1) // batch_size,
                )

                with ExifToolHelper() as et:
                    metadata_list = [
                        metadata
                        for metadata in et.get_metadata(batch, ["-api", "geolocation"])
                        if metadata
                    ]

                    if metadata_list:
                        normalized_metadata = [
                            normalize_exiftool_data(md) for md in metadata_list
                        ]

                        result = collection.insert_many(normalized_metadata)
                        logger.info(
                            "Successfully inserted %d photo metadata records",
                            len(result.inserted_ids),
                        )
                    else:
                        logger.warning(
                            "No metadata was extracted from the current batch",
                        )

            logger.info("EXIF extraction completed successfully")

        finally:
            client.close()

    except Exception:
        logger.exception("Error during processing")
        sys.exit(1)


if __name__ == "__main__":
    main()
