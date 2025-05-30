"""Generates a CSV only with the available media elements.

Reads MongoDB and select only the media elements that have GPS data.
Generates a CSV file with that.

Step 1.
"""

import csv
import sys
from pathlib import Path

from dotenv import load_dotenv

from db import get_mongodb_connection
from logger import get_logger, setup_logging
from utils.fs_utils import get_validated_path_from_env

logger = get_logger(__name__)


# Removed _get_validated_mongo_database_env (handled by db.py)
# Removed _get_validated_temp_image_file_env (replaced by get_validated_path_from_env)

def main() -> None:
    """Generate CSV with media elements that have GPS data."""
    try:
        # Setup logging
        setup_logging(__file__, log_directory="logs")

        # Load environment variables
        load_dotenv()

        # Validate environment variables
        try:
            # MONGO_DATABASE validation is handled by db.py via get_mongodb_connection()
            temp_image_file = get_validated_path_from_env(
                var_name="TEMP_IMAGE_FILE",
                purpose="temporary image file for CSV output",
                check_exists=False, # File might be created by this script
                check_is_file=False,
            )
        except (ValueError, FileNotFoundError, NotADirectoryError):
            # FileNotFoundError/NotADirectoryError are less likely here
            # with check_exists=False, check_is_file=False
            logger.exception("Environment variable or path validation failed")
            raise # Re-raise to be caught by the main try-except block

        # Note: The original code for temp_image_file did not check .is_file() here.
        # If that check is needed, it should be added separately and
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

    except Exception: # General exception handler
        logger.exception("CSV generation process failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
