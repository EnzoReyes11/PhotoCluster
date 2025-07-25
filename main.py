"""Generates a CSV only with the available media elements.

Reads MongoDB and select only the media elements that have GPS data.
Generates a CSV file with that.

Step 1.
"""

import csv
import sys
from pathlib import Path

from dotenv import load_dotenv
from pymongo.cursor import Cursor

from logger import get_logger, setup_logging
from utils.fs_utils import get_validated_path_from_env

load_dotenv()

from db import get_mongodb_connection  # noqa: E402

logger = get_logger(__name__)


def _write_output_file(file: Path, docs: Cursor, docs_count: int) -> None:
    with Path.open(file, "w", newline="") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(
            ["SourceFile", "GPSLatitude", "GPSLongitude", "GPSAltitude"],
        )

        for key in docs:
            csv_writer.writerow(
                [
                    key["SourceFile"],
                    key["GPSLatitude"],
                    key["GPSLongitude"],
                    key["GPSAltitude"],
                ],
            )
    logger.info(
        "Successfully wrote %d records to %s",
        docs_count,
        file,
    )


def main() -> None:
    """Generate CSV with media elements that have GPS data."""
    try:
        setup_logging(__file__, log_directory="logs")

        try:
            # If it doesn't exists, it will be created on writing.
            temp_image_file = get_validated_path_from_env(
                var_name="TEMP_IMAGE_FILE",
                purpose="temporary image file for CSV output",
                check_exists=False,
                check_is_file=False,
            )
        except (ValueError, FileNotFoundError, NotADirectoryError):
            logger.exception("Environment variable or path validation failed")
            raise

        client, collection = get_mongodb_connection()

        try:
            # Create indexes for efficient querying
            collection.create_index("cluster.id")
            collection.create_index("cluster.isCenter")
            collection.create_index("cluster.locationName")

            # Query documents with GPS data
            query = {
                "$and": [
                    {"GPSPosition": {"$ne": None}},
                    {"GPSAltitude": {"$ne": None}},
                ],
            }
            docs = collection.find(query)
            docs_count = collection.count_documents(query)

            _write_output_file(temp_image_file, docs, docs_count)

        finally:
            client.close()

    except Exception:
        logger.exception("CSV generation process failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
