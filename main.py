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


def _raise_value_error(message: str) -> None:
    """Raise a ValueError with the given message."""
    raise ValueError(message)


def main() -> None:
    """Generate CSV with media elements that have GPS data."""
    try:
        # Setup logging
        setup_logging(__file__, log_directory="logs")

        # Load environment variables
        load_dotenv()

        # Validate environment variables
        database = os.getenv("MONGO_DATABASE")
        temp_image_file = Path(os.getenv("TEMP_IMAGE_FILE", ""))

        if not database:
            _raise_value_error("MONGO_DATABASE environment variable is required")
        if not temp_image_file:
            _raise_value_error("TEMP_IMAGE_FILE environment variable is required")

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
