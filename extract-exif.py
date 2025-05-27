"""Extracts EXIF data from a directory tree and stores it in MongoDB."""

import os
import sys
from pathlib import Path

import pymongo
from dotenv import load_dotenv
from exiftool import ExifToolHelper

load_dotenv()

# MongoDB configuration
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", "27017"))
MONGO_DATABASE = os.getenv("MONGO_DATABASE")
SOURCE_IMAGES_DIR_PATH = os.getenv("SOURCE_IMAGES_DIR_PATH")
UNSUPPORTED_FILES_LOG = os.getenv("UNSUPPORTED_FILES_LOG", "unsupported_files.log")

# Validate required environment variables
required_env_vars = ["MONGO_DATABASE", "SOURCE_IMAGES_DIR_PATH"]
missing_env_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_env_vars:
    print(f"Missing required environment variables: {', '.join(missing_env_vars)}")
    sys.exit(1)

# MongoDB connection
try:
    client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
    client.admin.command("ping")  # Test connection
    db = client[MONGO_DATABASE]
    photos2 = db["photos2"]
except pymongo.errors.ServerSelectionTimeoutError as e:
    print(f"Error connecting to MongoDB: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Unexpected error when setting up MongoDB: {e}")
    sys.exit(1)


def read_all_files(directory: str) -> list[str]:
    """Read all supported media files from a directory tree.

    Args:
        directory: Root directory to start searching from

    Returns:
        List of absolute file paths for supported media files

    """
    file_paths: list[str] = []
    unsupported_extensions: set[str] = set()
    supported_extensions = {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".tiff",
        ".webp",
        ".mov",
        ".heic",
        ".mp4",
    }

    def read_all_files_rec(current_dir: Path) -> None:
        """Recursively read files from directory.

        Args:
            current_dir: Current directory to process

        """
        for item in current_dir.iterdir():
            if item.is_dir():
                read_all_files_rec(item)
            else:
                ext = item.suffix.lower()
                if ext in supported_extensions:
                    file_paths.append(str(item.absolute()))
                else:
                    unsupported_extensions.add(ext)

    # Start recursive file reading
    read_all_files_rec(Path(directory))

    # Log unsupported extensions
    if unsupported_extensions:
        with Path.open(UNSUPPORTED_FILES_LOG, "a") as log_file:
            log_file.write("\n".join(sorted(unsupported_extensions)) + "\n")

    return file_paths


def main() -> None:
    """Main function to process images and store metadata in MongoDB."""
    try:
        # Get all image files
        file_paths = read_all_files(SOURCE_IMAGES_DIR_PATH)
        print(f"Found {len(file_paths)} image files to process")

        # Process files in batches to avoid memory issues
        batch_size = 1000
        for i in range(0, len(file_paths), batch_size):
            batch = file_paths[i : i + batch_size]

            print(
                f"Processing batch {i // batch_size + 1} of "
                f"{(len(file_paths) + batch_size - 1) // batch_size}",
            )

            with ExifToolHelper() as et:
                metadata_list = [
                    metadata for metadata in et.get_metadata(batch) if metadata
                ]

                if metadata_list:
                    # Insert batch into MongoDB
                    result = photos2.insert_many(metadata_list)
                    print(
                        f"Successfully inserted {len(result.inserted_ids)} photo "
                        f"metadata records",
                    )
                else:
                    print("No metadata was extracted from the current batch")

    except Exception as e:
        print(f"Error during processing: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
