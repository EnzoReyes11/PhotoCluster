import os
import pymongo 
from dotenv import load_dotenv

load_dotenv()

MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DATABASE = os.getenv('MONGO_DATABASE')
TEMP_IMAGE_FILE = os.getenv('TEMP_IMAGE_FILE') 

myclient = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
db = myclient[MONGO_DATABASE]
photos = db["photos"]
query = {"$and": [{"GPSPosition": {"$ne": None}}, {"GPSAltitude": {"$ne": None}}]}

# Create indexes for efficient querying
photos.create_index("cluster.id")
photos.create_index("cluster.isCenter")
photos.create_index("cluster.locationName")

docs = photos.find(query)
docs_count = photos.count_documents(query)

print(docs_count)

with open(TEMP_IMAGE_FILE, "w") as f:
  f.write('SourceFile' + ',' + 'GPSLatitude' + ',' + 'GPSLongitude' + ',' + 'GPSAltitude' +  "\n")
  for key in docs:
    f.write(key['SourceFile'] + ',' + str(key['GPSLatitude']) + ',' + str(key['GPSLongitude']) + ',' + str(key['GPSAltitude']) +  "\n")