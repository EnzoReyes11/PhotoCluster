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
    port = int(os.getenv("MONGO_PORT", "27017"))
    database = os.getenv("MONGO_DATABASE")
    collection_name = os.getenv("MONGO_COLLECTION", "photos")

    if not database:
        raise ValueError("MONGO_DATABASE environment variable is required")

    client = pymongo.MongoClient(host, port)
    client.admin.command("ping")  # Test connection
    db = client[database]
    collection = db[collection_name]

    return client, collection
