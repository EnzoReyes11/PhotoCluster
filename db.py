"""Database connection utilities for the PhotoCluster application."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pymongo

from logger import get_logger
from utils.env_utils import get_required_env_var

if TYPE_CHECKING:
    from pymongo.collection import Collection

logger = get_logger(__name__)

# Fetch and validate required environment variables at module level
MONGO_HOST_VALIDATED = get_required_env_var("MONGO_HOST", "MongoDB host")
MONGO_PORT_STR_VALIDATED = get_required_env_var("MONGO_PORT", "MongoDB port")
MONGO_DATABASE_NAME_VALIDATED = get_required_env_var(
    "MONGO_DATABASE", "MongoDB database name",
)

try:
    MONGO_PORT_VALIDATED = int(MONGO_PORT_STR_VALIDATED)
except ValueError as e:
    # This error handling is crucial at module load time
    logger.exception(
        "MONGO_PORT must be a valid integer, got: '%s'", MONGO_PORT_STR_VALIDATED,
    )
    msg = f"Invalid MONGO_PORT: '{MONGO_PORT_STR_VALIDATED}'. Must be an integer."
    raise ValueError( # Re-raise as ValueError to prevent application startup
        msg,
    ) from e


def get_mongodb_connection() -> tuple[pymongo.MongoClient, Collection]:
    """Establish connection to MongoDB using pre-validated environment variables.

    Returns:
        Tuple of (MongoDB client, collection)

    Raises:
        pymongo.errors.ServerSelectionTimeoutError: If cannot connect to MongoDB
        Exception: For other unexpected errors during connection or ping

    """
    # MONGO_HOST, MONGO_PORT, MONGO_DATABASE are now validated module-level variables
    collection_name = os.getenv("MONGO_COLLECTION", "photos") # Remains as is

    client = pymongo.MongoClient(MONGO_HOST_VALIDATED, MONGO_PORT_VALIDATED)
    try:
        client.admin.command("ping")  # Test connection
        db = client[MONGO_DATABASE_NAME_VALIDATED]
        collection = db[collection_name]
    except Exception: # Covers ConnectionFailure, ServerSelectionTimeoutError, etc.
        client.close()
        raise

    return client, collection
