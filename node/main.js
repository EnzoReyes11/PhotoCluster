/**
 * Retrieve the exif metadata from the speciefied directory.
 *
 * Loads the exif metadata of the specified directory into a MongoDB collection .
 *
 * Step 0
 */
const fs = require("fs");
const path = require("path");
const Exif = require("simple-exiftool");
require("dotenv").config({ path: "../.env" });

const { MongoClient } = require("mongodb");

const EventEmitter = require("node:events");

class ExifEmitter extends EventEmitter {}

const exifEmitter = new ExifEmitter();

const MONGO_HOST = process.env.MONGO_HOST;
const MONGO_PORT = process.env.MONGO_PORT;
const MONGO_DATABASE = process.env.MONGO_DATABASE;
const SOURCE_IMAGES_DIR_PATH = process.env.SOURCE_IMAGES_DIR_PATH;
const UNSUPPORTED_FILES_LOG = process.env.UNSUPPORTED_FILES_LOG || path.join(__dirname, "unsupported_files.log");

const requiredEnvVars = ["MONGO_HOST", "MONGO_PORT", "MONGO_DATABASE", "SOURCE_IMAGES_DIR_PATH"];
const missingEnvVars = requiredEnvVars.filter((varName) => !process.env[varName]);

if (missingEnvVars.length > 0) {
  console.error(`Missing required environment variables: ${missingEnvVars.join(", ")}`);
  process.exit(1);
}

const uri = `mongodb://${MONGO_HOST}:${MONGO_PORT}`;

const client = new MongoClient(uri);

function readAllFiles(dir) {
  let filePaths = [];
  let unsupportedExtensions = new Set();

  function readAllFilesRec(dir) {
    const files = fs.readdirSync(dir, { withFileTypes: true });
    const supportedExtensions = [".jpg", ".jpeg", ".png", ".gif", ".tiff", ".webp", ".mov", ".heic", ".mp4"];

    for (const file of files) {
      if (file.isDirectory()) {
        readAllFilesRec(path.join(dir, file.name));
      } else {
        const ext = path.extname(file.name).toLowerCase();
        if (supportedExtensions.includes(ext)) {
          filePaths.push(path.join(dir, file.name));
        } else {
          unsupportedExtensions.add(ext);
        }
      }
    }
  }

  readAllFilesRec(dir);

  // Write unsupported extensions to log file
  if (unsupportedExtensions.size > 0) {
    const logContent = Array.from(unsupportedExtensions).join("\n") + "\n";
    fs.appendFileSync(UNSUPPORTED_FILES_LOG, logContent);
  }

  return filePaths;
}

exifEmitter.on("done", async (args) => {
  try {
    await client.connect();

    const db = client.db(MONGO_DATABASE);
    const collection = db.collection("photos");
    await collection.insertMany(args);

    console.log(`Successfully inserted ${args.length} photo metadata records`);
  } catch (error) {
    console.error("Error during MongoDB operations:", error.message);
    if (error.name === "MongoServerSelectionError") {
      console.error("MongoDB connection failed. Please check your MongoDB connection parameters.");
    } else if (error.name === "MongoBulkWriteError") {
      console.error("Error during bulk write operation. Some documents may have been inserted.");
    }
  } finally {
    await client.close();
  }
});

async function main() {
  let images = readAllFiles(SOURCE_IMAGES_DIR_PATH);
  console.log(`Found ${images.length} image files to process`);

  Exif(images, (error, metadataArray) => {
    if (error) {
      console.error("Error during EXIF extraction:", error);
      return;
    }

    if (!metadataArray || metadataArray.length === 0) {
      console.warn("No metadata was extracted from the images");
      return;
    }

    console.log(`Successfully extracted metadata from ${metadataArray.length} images`);
    exifEmitter.emit("done", metadataArray);
  });
}

main().catch(console.dir);
