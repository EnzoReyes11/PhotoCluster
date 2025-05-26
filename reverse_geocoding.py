"""Retrieves the location for each cluster.

Runs reverse Geocoding for each cluster, and updates the MongoDB collection with
this information.
"""

import os
import sys

import googlemaps
import pymongo
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "your_api_key")
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "photoLocator")


if not API_KEY or API_KEY == "your_api_key":
    raise ValueError("Valid GOOGLE_MAPS_API_KEY environment variable is required")
if not MONGO_DATABASE:
    raise ValueError("MONGO_DATABASE environment variable is required")

# Find all center photos that need reverse geocoding
try:
    myclient = pymongo.MongoClient(
        MONGO_HOST,
        MONGO_PORT,
    )
    myclient.admin.command("ping")

    db = myclient[MONGO_DATABASE]
    photos = db["photos"]

    query = {"cluster.isCenter": True}
    center_count = photos.count_documents(query)
    center_photos = photos.find(query)

    print(f"Found {center_count} center photos to process")
except pymongo.errors.ServerSelectionTimeoutError as e:
    print(f"Error connecting to MongoDB: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Unexpected error when setting up MongoDB: {e}")
    sys.exit(1)


try:
    gmaps = googlemaps.Client(key=API_KEY)
except Exception as e:
    print(f"Error initializing Google Maps client: {e}")
    gmaps = None

location_names = {}

if gmaps:
    print("Processing center photos coordinates...")
    for photo in center_photos:
        try:
            lat = float(photo["GPSLatitude"])
            lon = float(photo["GPSLongitude"])
        except (TypeError, ValueError, KeyError):
            print(f"  -> Skipping document {photo.get('_id')} – invalid GPS data")
            continue
        coordinate_tuple = (lat, lon)

        print(f"\nLooking up coordinates: {coordinate_tuple}")
        try:
            reverse_geocode_result = gmaps.reverse_geocode(
                coordinate_tuple,
                result_type="political",
            )

            if reverse_geocode_result:
                location_name = reverse_geocode_result[0]["formatted_address"]
                print(f"  -> Found Location: {location_name}")

                photos.update_many(
                    {"cluster.id": photo["cluster"]["id"]},
                    {"$set": {"cluster.locationName": location_name}},
                )
            else:
                print(f"  -> No address found for coordinates: {coordinate_tuple}")

        except googlemaps.exceptions.ApiError as e:
            print(f"  -> Google Maps API Error for {coordinate_tuple}: {e}")
        except Exception as e:
            print(f"  -> An unexpected error occurred for {coordinate_tuple}: {e}")

    print("\n--- Processing Complete ---")
else:
    print("Could not proceed without a valid Google Maps client.")
