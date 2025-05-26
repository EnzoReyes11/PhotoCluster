"""Generates a CSV only with the available media elements.

Reads MongoDB and select only the media elements that have GPS data.
Generates a CSV file with that.

Step 1.
"""

import csv
import os
import sys

import pymongo
from dotenv import load_dotenv

load_dotenv()

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DATABASE = os.getenv("MONGO_DATABASE")
TEMP_IMAGE_FILE = os.getenv("TEMP_IMAGE_FILE")

# Validate required environment variables
if not MONGO_DATABASE:
    raise ValueError("MONGO_DATABASE environment variable is required")
if not TEMP_IMAGE_FILE:
    raise ValueError("TEMP_IMAGE_FILE environment variable is required")

try:
    myclient = pymongo.MongoClient(
        MONGO_HOST,
        MONGO_PORT,
    )
    myclient.admin.command("ping")
    db = myclient[MONGO_DATABASE]
    photos = db["photos"]
except pymongo.errors.ServerSelectionTimeoutError as e:
    print(f"Error connecting to MongoDB: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Unexpected error when setting up MongoDB: {e}")
    sys.exit(1)

# Create indexes for efficient querying
photos.create_index("cluster.id")
photos.create_index("cluster.isCenter")
photos.create_index("cluster.locationName")

query = {"$and": [{"GPSPosition": {"$ne": None}}, {"GPSAltitude": {"$ne": None}}]}
docs = photos.find(query)
docs_count = photos.count_documents(query)

print(docs_count)

try:
    with open(TEMP_IMAGE_FILE, "w", newline="") as f:
        csv_writer = csv.writer(f)
        # Write header
        csv_writer.writerow(
            ["SourceFile", "GPSLatitude", "GPSLongitude", "GPSAltitude"],
        )
        # Write data
        for key in docs:
            csv_writer.writerow(
                [
                    key["SourceFile"],
                    key["GPSLatitude"],
                    key["GPSLongitude"],
                    key["GPSAltitude"],
                ],
            )
    print(f"Successfully wrote {docs_count} records to {TEMP_IMAGE_FILE}")
except OSError as e:
    print(f"Error writing to CSV file: {e}")
except Exception as e:
    print(f"Unexpected error during CSV export: {e}")
