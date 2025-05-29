"""Database connection utilities for the PhotoCluster application."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pymongo

from logger import get_logger

if TYPE_CHECKING:
    from pymongo.collection import Collection

logger = get_logger(__name__)


def get_mongodb_connection() -> tuple[pymongo.MongoClient, Collection]:
    """Establish connection to MongoDB.

    Returns:
        Tuple of (MongoDB client, collection)

    Raises:
        pymongo.errors.ServerSelectionTimeoutError: If cannot connect to MongoDB
        ValueError: If required environment variables are missing
        Exception: For other unexpected errors

    """
    host = os.getenv("MONGO_HOST", "localhost")
    port_str = os.getenv("MONGO_PORT", "27017")

    try:
        port = int(port_str)
    except ValueError as err:
        msg = f"MONGO_PORT must be a valid integer, got: {port_str}"
        raise ValueError(msg) from err

    database = os.getenv("MONGO_DATABASE")
    collection_name = os.getenv("MONGO_COLLECTION", "photos")

    if not database:
        msg = "MONGO_DATABASE environment variable is required"
        raise ValueError(msg)

    client = pymongo.MongoClient(host, port)
    try:
        client.admin.command("ping")  # Test connection
        db = client[database]
        collection = db[collection_name]
    except Exception:
        client.close()
        raise

    return client, collection
