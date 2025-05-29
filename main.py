"""Generates a CSV only with the available media elements.

Reads MongoDB and select only the media elements that have GPS data.
Generates a CSV file with that.

Step 1.
"""

import csv
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from db import get_mongodb_connection
from logger import get_logger, setup_logging

logger = get_logger(__name__)


def _get_validated_mongo_database_env() -> str:
    """Get and validate the MONGO_DATABASE environment variable."""
    database = os.getenv("MONGO_DATABASE")
    if not database:
        msg = "MONGO_DATABASE environment variable is required"
        raise ValueError(msg)
    return database


def _get_validated_temp_image_file_env() -> Path:
    """Get and validate the TEMP_IMAGE_FILE environment variable."""
    temp_image_file_str = os.getenv("TEMP_IMAGE_FILE")
    if not temp_image_file_str:
        msg = "TEMP_IMAGE_FILE environment variable is required"
        raise ValueError(msg)
    return Path(temp_image_file_str)


def main() -> None:
    """Generate CSV with media elements that have GPS data."""
    try:
        # Setup logging
        setup_logging(__file__, log_directory="logs")

        # Load environment variables
        load_dotenv()

        # Validate environment variables
        try:
            _get_validated_mongo_database_env()
            temp_image_file = _get_validated_temp_image_file_env()
        except ValueError:
            logger.exception("Configuration error")
            raise # Re-raise to be caught by the main try-except block

        # Note: The original code for temp_image_file did not check .is_file()
        # here. If that check is needed, it should be added separately and
        # might raise FileNotFoundError. # Shortened comment

        # Connect to MongoDB
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

            # Write to CSV
            with Path.open(temp_image_file, "w", newline="") as f:
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
                temp_image_file,
            )

        finally:
            client.close()

    except Exception:
        logger.exception("Error during processing")
        sys.exit(1)


if __name__ == "__main__":
    main()
