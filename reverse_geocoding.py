# Install the Google Maps client library if you haven't already
# Run this command in a Colab cell:

import os
from dotenv import load_dotenv
import pymongo
import googlemaps
from datetime import datetime

load_dotenv()

API_KEY = os.getenv('GOOGLE_MAPS_API_KEY', 'your_api_key')
MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'photoLocator')

myclient = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
db = myclient[MONGO_DATABASE]
photos = db["photos"]

# Find all center photos that need reverse geocoding
query = {"cluster.isCenter": True}
center_photos = photos.find(query)
print(f"Found {photos.count_documents(query)} center photos to process")

# --- Initialize Google Maps Client ---
try:
    gmaps = googlemaps.Client(key=API_KEY)
except Exception as e:
    print(f"Error initializing Google Maps client: {e}")
    # Exit or handle the error appropriately if the client can't be initialized
    gmaps = None

# --- Process Coordinates ---
location_names = {} # Dictionary to store results {coordinate_tuple: location_name}

if gmaps: # Proceed only if the client was initialized successfully
    print(f"Processing center photos coordinates...")
    for photo in center_photos:
        coordinate_tuple = (float(photo['GPSLatitude']), float(photo['GPSLongitude']))
        print(f"\nLooking up coordinates: {coordinate_tuple}")
        try:
            reverse_geocode_result = gmaps.reverse_geocode(coordinate_tuple, result_type='political')

            if reverse_geocode_result:
                location_name = reverse_geocode_result[0]['formatted_address']
                print(f"  -> Found Location: {location_name}")
                # Update the center photo
                photos.update_one(
                    {"_id": photo['_id']},
                    {"$set": {"cluster.locationName": location_name}}
                )
                
                # Update all photos in the same cluster
                photos.update_many(
                    {"cluster.id": photo['cluster']['id']},
                    {"$set": {"cluster.locationName": location_name}}
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