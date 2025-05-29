"""Retrieves the location for each cluster.

Runs reverse Geocoding for each cluster, and updates the MongoDB collection with
this information.
"""

import logging
import os
import sys
from pathlib import Path

import googlemaps
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


def get_mongodb_connection() -> tuple[
    pymongo.MongoClient, pymongo.collection.Collection
]:
    """Establish connection to MongoDB.

    Returns:
        Tuple of (MongoDB client, collection)

    Raises:
        pymongo.errors.ServerSelectionTimeoutError: If cannot connect to MongoDB
        Exception: For other unexpected errors

    """
    host = os.getenv("MONGO_HOST", "localhost")
    port = int(os.getenv("MONGO_PORT", "27017"))
    database = os.getenv("MONGO_DATABASE")
    collection_name = os.getenv("MONGO_COLLECTION", "photos")

    if not database:
        raise ValueError("MONGO_DATABASE environment variable is required")

    client = pymongo.MongoClient(host, port)
    client.admin.command("ping")  # Test connection
    db = client[database]
    collection = db[collection_name]

    return client, collection


def main() -> None:
    """Run reverse geocoding for cluster centers and update MongoDB records."""
    try:
        # Setup logging
        setup_logging()

        # Load environment variables
        load_dotenv()

        # Validate environment variables
        api_key = os.getenv("GOOGLE_MAPS_API_KEY", "your_api_key")
        database = os.getenv("MONGO_DATABASE")

        if not api_key or api_key == "your_api_key":
            raise ValueError(
                "Valid GOOGLE_MAPS_API_KEY environment variable is required"
            )
        if not database:
            raise ValueError("MONGO_DATABASE environment variable is required")

        # Connect to MongoDB
        client, collection = get_mongodb_connection()

        try:
            # Find all center photos that need reverse geocoding
            query = {"cluster.isCenter": True}
            center_count = collection.count_documents(query)
            center_photos = collection.find(query)

            logger.info("Found %d center photos to process", center_count)

            # Initialize Google Maps client
            try:
                gmaps = googlemaps.Client(key=api_key)
            except Exception:
                logger.exception("Error initializing Google Maps client")
                return

            # Process each center photo
            for photo in center_photos:
                try:
                    lat = float(photo["GPSLatitude"])
                    lon = float(photo["GPSLongitude"])
                except (TypeError, ValueError, KeyError):
                    logger.warning(
                        "Skipping document %s – invalid GPS data",
                        photo.get("_id"),
                    )
                    continue

                coordinate_tuple = (lat, lon)
                logger.info("Looking up coordinates: %s", coordinate_tuple)

                try:
                    reverse_geocode_result = gmaps.reverse_geocode(
                        coordinate_tuple,
                        result_type="political",
                    )

                    if reverse_geocode_result:
                        location_name = reverse_geocode_result[0]["formatted_address"]
                        logger.info("Found Location: %s", location_name)

                        collection.update_many(
                            {"cluster.id": photo["cluster"]["id"]},
                            {"$set": {"cluster.locationName": location_name}},
                        )
                    else:
                        logger.warning(
                            "No address found for coordinates: %s",
                            coordinate_tuple,
                        )

                except googlemaps.exceptions.ApiError:
                    logger.exception(
                        "Google Maps API Error for %s",
                        coordinate_tuple,
                    )
                except Exception:
                    logger.exception(
                        "An unexpected error occurred for %s",
                        coordinate_tuple,
                    )

            logger.info("Reverse geocoding completed successfully")

        finally:
            client.close()

    except Exception:
        logger.exception("Error during processing")
        sys.exit(1)


if __name__ == "__main__":
    main()
