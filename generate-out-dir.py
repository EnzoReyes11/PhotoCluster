"""Generates a directory tree with the media files cluster.

Given a directory path, creates subdirectories for each cluster center
(with its cluster.locationName as name) and creates symlinks to the actual
media files for each cluster member.

Step 3.
"""

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

from db import get_mongodb_connection
from logger import get_logger, setup_logging

if TYPE_CHECKING:
    from pymongo.collection import Collection

logger = get_logger(__name__)


def create_safe_dirname(name: str) -> str:
    """Create a safe directory name from a location name.

    Args:
        name: Location name to convert to directory name

    Returns:
        Safe directory name

    """
    # Replace problematic characters with underscores
    safe_name = "".join(c if c.isalnum() or c in " -_" else "_" for c in name)
    # Replace spaces with underscores
    safe_name = safe_name.replace(" ", "_")
    # Remove multiple consecutive underscores
    while "__" in safe_name:
        safe_name = safe_name.replace("__", "_")
    return safe_name


def get_next_directory_number(base_dir: Path, algorithm: str, separator: str) -> int:
    """Find the next available directory number for the clustering algorithm.

    Args:
        base_dir: Base directory to search in
        algorithm: Name of the clustering algorithm
        separator: Char to separate the numeric suffix

    Returns:
        Next available directory number (1 if no existing directories found)

    """
    if not base_dir.exists():
        return 1

    # Find all directories matching the pattern algorithm_*
    existing_dirs = [
        d.name
        for d in base_dir.iterdir()
        if d.is_dir() and d.name.startswith(f"{algorithm}{separator}")
    ]

    if not existing_dirs:
        return 1

    # Extract numbers from directory names using list comprehension
    numbers = [
        int(dir_name.split(separator)[-1])
        for dir_name in existing_dirs
        if dir_name.split(separator)[-1].isdigit()
    ]

    return max(numbers, default=0) + 1


def create_cluster_directories(
    collection: "Collection",
    output_dir: Path,
) -> None:
    """Create directories for each cluster and symlink media files.

    Args:
        collection: MongoDB collection containing cluster data
        output_dir: Directory where cluster directories will be created

    """
    # Find all clusters with location names
    query = {
        "$and": [
            {"cluster.isCenter": True},
            {"cluster.locationName": {"$ne": None}},
        ],
    }
    clusters = collection.find(query)
    total_clusters = collection.count_documents(query)
    logger.info("Found %d clusters to process", total_clusters)

    for cluster in clusters:
        try:
            location_name = cluster["cluster"]["locationName"]
            cluster_id = cluster["cluster"]["id"]

            # Create safe directory name
            dir_name = create_safe_dirname(location_name)
            cluster_dir = output_dir / dir_name

            logger.info(
                "Processing cluster %d: %s -> %s",
                cluster_id,
                location_name,
                cluster_dir,
            )

            # Create cluster directory
            cluster_dir.mkdir(exist_ok=True)

            # Find all members of this cluster
            members_query = {"cluster.id": cluster_id}
            members = collection.find(members_query)

            # Create symlinks for each member
            for member in members:
                source_file = Path(member["SourceFile"])
                if not source_file.exists():
                    logger.warning(
                        "Source file does not exist: %s",
                        source_file,
                    )
                    continue

                # Create symlink
                target = cluster_dir / Path(source_file).name
                if not target.exists():
                    try:
                        os.symlink(source_file, target)
                        logger.debug(
                            "Created symlink: %s -> %s",
                            target,
                            source_file,
                        )
                    except OSError:
                        logger.exception(
                            "Error creating symlink for %s",
                            source_file,
                        )
                else:
                    logger.debug(
                        "Symlink already exists: %s",
                        target,
                    )

        except KeyError:
            logger.exception(
                "Missing required field in cluster document",
            )
            continue
        except Exception:
            logger.exception(
                "Error processing cluster %s",
                cluster.get("_id"),
            )
            continue


def main() -> None:
    """Organize media files into cluster directories."""
    try:
        # Setup logging
        setup_logging(__file__, log_directory="logs")

        # Load environment variables
        load_dotenv()

        # Get base output directory and clustering algorithm
        base_output_dir = Path(os.getenv("OUTPUT_DIR_PATH", ""))
        algorithm = os.getenv("CLUSTERING_ALGORITHM")

        if not base_output_dir:
            raise ValueError("OUTPUT_DIR_PATH environment variable is required")
        if not algorithm:
            raise ValueError("CLUSTERING_ALGORITHM environment variable is required")

        # Create base directory if it doesn't exist
        if not base_output_dir.exists():
            logger.info("Creating base output directory: %s", base_output_dir)
            base_output_dir.mkdir(parents=True, exist_ok=True)

        # Get next directory number and create output directory
        separator = "-"
        dir_number = get_next_directory_number(base_output_dir, algorithm, separator)
        output_dir = base_output_dir / f"{algorithm}{separator}{dir_number}"

        logger.info("Creating output directory: %s", output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Connect to MongoDB
        client, collection = get_mongodb_connection()

        try:
            logger.info("Starting cluster organization in %s", output_dir)
            create_cluster_directories(collection, output_dir)
            logger.info("Finished cluster organization")
        finally:
            client.close()

    except Exception:
        logger.exception("Error during processing")
        sys.exit(1)


if __name__ == "__main__":
    main()
