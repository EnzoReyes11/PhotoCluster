"""Generates a CSV only with the available media elements.

Reads MongoDB and select only the media elements that have GPS data.
Generates a CSV file with that.

Step 1.
"""

import csv
import logging
import os
import sys
from pathlib import Path

import pymongo
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure logging for the application."""
    current_file = Path(__file__).stem
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"{current_file}.log"),
        ],
    )


def main() -> None:
    """Generate CSV with media elements that have GPS data."""
    try:
        # Setup logging
        setup_logging()

        # Load environment variables
        load_dotenv()

        # Validate environment variables
        database = os.getenv("MONGO_DATABASE")
        temp_image_file = Path(os.getenv("TEMP_IMAGE_FILE", ""))

        if not database:
            raise ValueError("MONGO_DATABASE environment variable is required")
        if not temp_image_file:
            raise ValueError("TEMP_IMAGE_FILE environment variable is required")

        # Connect to MongoDB
        client = pymongo.MongoClient(
            os.getenv("MONGO_HOST", "localhost"),
            int(os.getenv("MONGO_PORT", "27017")),
        )
        client.admin.command("ping")
        db = client[database]
        collection = db[os.getenv("MONGO_COLLECTION", "photos")]

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

    except pymongo.errors.ServerSelectionTimeoutError:
        logger.exception("Error connecting to MongoDB")
        sys.exit(1)
    except OSError:
        logger.exception("Error writing to CSV file")
    except Exception:
        logger.exception("Error during processing")
        sys.exit(1)


if __name__ == "__main__":
    main()
