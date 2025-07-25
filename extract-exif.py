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

from logger import get_logger, setup_logging
from utils.fs_utils import get_validated_path_from_env

load_dotenv()


from db import get_mongodb_connection  # noqa: E402

logger = get_logger(__name__)


def _log_unsupported_extensions(
    unsupported_extensions: set[str],
    unsupported_files_log: Path,
) -> None:
    if unsupported_extensions:
        timestamp = datetime.now(timezone.utc).isoformat()
        with Path.open(unsupported_files_log, "a") as log_file:
            log_file.write(
                f"{timestamp}: {', '.join(sorted(unsupported_extensions))}\n",
            )


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

    _log_unsupported_extensions(unsupported_extensions, unsupported_files_log)

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


def main() -> None:
    """Extract EXIF data from supported media and store metadata in MongoDB."""
    try:
        setup_logging(__file__, log_directory="logs")

        try:
            source_dir = get_validated_path_from_env(
                var_name="SOURCE_IMAGES_DIR_PATH",
                purpose="source directory for images",
                check_exists=True,
                check_is_dir=True,
            )
        except (ValueError, FileNotFoundError, NotADirectoryError):
            logger.exception("Environment variable or path validation failed")
            raise

        unsupported_files_log = Path(
            os.getenv("UNSUPPORTED_FILES_LOG", "unsupported_files.log"),
        )

        client, collection = get_mongodb_connection()

        try:
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
        logger.exception("EXIF extraction process failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
