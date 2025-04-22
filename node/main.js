const fs = require('fs');
const path = require('path');
const Exif = require("simple-exiftool");
require('dotenv').config({ path: '../.env' })

const {MongoClient} = require('mongodb');

const EventEmitter = require('node:events');

class ExifEmitter extends EventEmitter {};

const exifEmitter = new ExifEmitter();

const MONGO_HOST = process.env.MONGO_HOST;
const MONGO_PORT = process.env.MONGO_PORT;
const MONGO_DATABASE = process.env.MONGO_DATABASE
const SOURCE_IMAGES_DIR_PATH = process.env.SOURCE_IMAGES_DIR_PATH;

// Connection URI
const uri =
  `mongodb://${MONGO_HOST}:${MONGO_PORT}`;
// Create a new MongoClient
const client = new MongoClient(uri);

function readAllFiles(dir) {
  let filePaths = [];
 
  return readAllFilesRec(dir, filePaths);
}

function readAllFilesRec(dir, filePaths) {
  const files = fs.readdirSync(dir, { withFileTypes: true });

  for (const file of files) {
    if (file.isDirectory()) {
      readAllFilesRec(path.join(dir, file.name), filePaths);
    } else {
      filePaths.push(path.join(dir, file.name));
    }
  }

  return filePaths;
}

exifEmitter.on('done', async (args) => {
    try {
    // Connect the client to the server
    await client.connect();
    // Establish and verify connection
    const db = client.db(MONGO_DATABASE);
    const collection = db.collection('photos')
    await collection.insertMany(args);
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }

})


async function main() {

  let images = readAllFiles(SOURCE_IMAGES_DIR_PATH);

  Exif(images, (error, metadataArray) => {
    exifEmitter.emit('done', metadataArray); 
  });
}

main().catch(console.dir);

