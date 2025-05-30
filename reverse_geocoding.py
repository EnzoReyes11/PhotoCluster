"""Retrieves the location for each cluster.

Runs reverse Geocoding for each cluster, and updates the MongoDB collection with
this information.
"""

import sys

import googlemaps
from dotenv import load_dotenv

from db import get_mongodb_connection
from logger import get_logger, setup_logging
from utils.env_utils import get_required_env_var

logger = get_logger(__name__)


def _raise_if_api_key_is_placeholder(api_key: str) -> None:
    """Raise ValueError if the provided API key is the placeholder value."""
    if api_key == "your_api_key":
        # This message is intentionally a bit long for the E501 fix demonstration
        err_msg = (
            "Placeholder GOOGLE_MAPS_API_KEY ('your_api_key') must be replaced"
            " with a valid key."
        )
        raise ValueError(err_msg)


def main() -> None:
    """Run reverse geocoding for cluster centers and update MongoDB records."""
    try:
        setup_logging(__file__, log_directory="logs")

        load_dotenv()

        try:
            api_key = get_required_env_var(
                var_name="GOOGLE_MAPS_API_KEY",
                purpose="Google Maps API access",
            )
            _raise_if_api_key_is_placeholder(api_key)
        except ValueError:
            logger.exception("Environment variable validation failed")
            raise

        client, collection = get_mongodb_connection()

        try:
            query = {"cluster.isCenter": True}
            center_count = collection.count_documents(query)
            center_photos = collection.find(query)

            logger.info("Found %d center photos to process", center_count)

            try:
                gmaps = googlemaps.Client(key=api_key)
            except Exception:
                logger.exception("Error initializing Google Maps client")
                return

            for photo in center_photos:
                try:
                    lat = float(photo["GPSLatitude"])
                    lon = float(photo["GPSLongitude"])
                except (TypeError, ValueError, KeyError):
                    logger.warning(
                        "Skipping document %s - invalid GPS data",
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
        logger.exception("Reverse geocoding process failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
