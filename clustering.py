"""Runs the clustering algorithm.

Reads the CSV file and runs the clustering algorithm. Currently Affinity Propagation.
Updates each media element record in the MongoDB collection with the cluster
information.

Step 2.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from geopy.distance import geodesic
from haversine import Unit, haversine
from sklearn import metrics
from sklearn.cluster import AffinityPropagation
from sklearn.metrics.pairwise import pairwise_distances

from db import get_mongodb_connection
from logger import get_logger, setup_logging

if TYPE_CHECKING:
    from pymongo.collection import Collection

logger = get_logger(__name__)


def calculate_distances(coords: np.ndarray, method: str = "haversine") -> np.ndarray:
    """Calculate distances between points using the specified method.

    Args:
        coords: Array of coordinates
        method: Distance calculation method ("geodesic" or "haversine")

    Returns:
        Distance matrix

    Raises:
        ValueError: If method is not supported

    """
    if method == "geodesic":
        logger.info("Calculating pairwise Geodesic distances...")

        def geodesic_wrapper(
            point1: tuple[float, float],
            point2: tuple[float, float],
        ) -> float:
            return geodesic(point1, point2).km

        distance_matrix = pairwise_distances(coords, metric=geodesic_wrapper)
    elif method == "haversine":
        logger.info("Calculating pairwise Haversine distances...")

        # haversine expects (lat, lon) tuples and returns distance in km
        def haversine_wrapper(
            point1: tuple[float, float],
            point2: tuple[float, float],
        ) -> float:
            return haversine(point1, point2, unit=Unit.KILOMETERS)

        distance_matrix = pairwise_distances(coords, metric=haversine_wrapper)
    else:
        msg = f"Unknown distance calculation method: {method}"
        raise ValueError(msg)

    return distance_matrix


def _parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Photo clustering with different distance calculation methods",
    )
    parser.add_argument(
        "--method",
        choices=["geodesic", "haversine"],
        default="haversine",
        help="Distance calculation method to use",
    )
    return parser.parse_args()


def _ensure_mongo_database_env_var_present() -> str:
    """Ensure MONGO_DATABASE environment variable is present and return it."""
    database = os.getenv("MONGO_DATABASE")
    if not database:
        msg = "MONGO_DATABASE environment variable is required"
        raise ValueError(msg)
    return database


def _ensure_temp_image_file_env_var_present() -> Path:
    """Ensure TEMP_IMAGE_FILE environment variable is present and return it."""
    temp_image_file_path_str = os.getenv("TEMP_IMAGE_FILE")
    if not temp_image_file_path_str:
        msg = "TEMP_IMAGE_FILE environment variable is required"
        raise ValueError(msg)
    return Path(temp_image_file_path_str)


def _ensure_file_exists(path: Path, file_description: str) -> None:
    """Ensure path exists and is a file, raising FileNotFoundError otherwise."""
    if not path.is_file():
        msg = f"{file_description} ({path}) does not exist or is not a file."
        raise FileNotFoundError(msg)


def _load_and_validate_env_vars() -> tuple[str, Path]:
    """Load and validate environment variables."""
    load_dotenv()
    # The try-except block here is to demonstrate catching errors from helpers
    # and potentially re-raising or handling them as needed.
    # For this specific refactoring, direct calls without try-except in this function
    # would also work if the main function's try-except is deemed sufficient.
    try:
        database = _ensure_mongo_database_env_var_present()
        temp_image_file = _ensure_temp_image_file_env_var_present()

        # Explicitly check if temp_image_file is a file, after ensuring var is present.
        _ensure_file_exists(temp_image_file, "TEMP_IMAGE_FILE")

    except ValueError: # Catching specific errors from helpers
        logger.exception("Configuration error")
        raise # Re-raise to be caught by the main try-except block
    except FileNotFoundError:
        logger.exception("File system error")
        raise # Re-raise to be caught by the main try-except block

    return database, temp_image_file


def _load_data(temp_image_file: Path) -> tuple[pd.DataFrame, np.ndarray]:
    """Load data from CSV file."""
    coords_df = pd.read_csv(temp_image_file)
    coords = coords_df[coords_df.columns[1:3]].to_numpy()
    logger.info("Loaded %d coordinates from CSV", len(coords))
    return coords_df, coords


def _perform_clustering(
    coords: np.ndarray,
    method: str,
) -> AffinityPropagation:
    """Perform affinity propagation clustering."""
    logger.info("Using %s distance calculation method", method)
    distance_matrix = calculate_distances(coords, method=method)
    similarity_matrix = -distance_matrix

    af = AffinityPropagation(
        affinity="precomputed",
        damping=0.9,
        max_iter=500,
        random_state=42,
    )
    af.fit(similarity_matrix)
    return af


def _update_database(
    collection: Collection,
    coords_df: pd.DataFrame,
    labels: np.ndarray,
    cluster_centers_indices: np.ndarray | None,
) -> None:
    """Update MongoDB with clustering results."""
    n_clusters = (
        len(cluster_centers_indices) if cluster_centers_indices is not None else 0
    )
    coords_df["cluster_id"] = labels

    if cluster_centers_indices is not None and len(cluster_centers_indices) > 0:
        for cluster_id in range(n_clusters):
            center_index = cluster_centers_indices[cluster_id]
            center_point_details = coords_df.iloc[center_index]

            cluster_members_indices = np.where(labels == cluster_id)[0]
            member_source_files = [
                coords_df.iloc[idx]["SourceFile"] for idx in cluster_members_indices
            ]

            if member_source_files:
                update_result = collection.update_many(
                    {"SourceFile": {"$in": member_source_files}},
                    {
                        "$set": {
                            "cluster": {
                                "id": cluster_id,
                                "isCenter": False,
                                "locationName": None,
                            },
                        },
                    },
                )
                logger.debug(
                    "Updated %d non-center members for cluster %d",
                    update_result.modified_count,
                    cluster_id,
                )

            collection.update_one(
                {"SourceFile": center_point_details["SourceFile"]},
                {
                    "$set": {
                        "cluster": {
                            "id": cluster_id,
                            "isCenter": True,
                            "locationName": None,
                        },
                    },
                },
            )
            logger.debug(
                "Updated center photo for cluster %d: %s",
                cluster_id,
                center_point_details["SourceFile"],
            )
    elif hasattr(labels, "size") and np.unique(labels).size > 0:
        # Check if labels exist and are not empty
        unique_labels = np.unique(labels)
        n_clusters_from_labels = len(unique_labels)  # Use a different variable name
        logger.warning(
            "Affinity Propagation converged, but cluster centers might be ambiguous.",
        )
        logger.info(
            "Estimated number of clusters based on labels: %d",
            n_clusters_from_labels, # Use the new variable name
        )
        for cluster_id in unique_labels:
            cluster_members_indices = np.where(labels == cluster_id)[0]
            coords_df.iloc[cluster_members_indices]
            logger.info(
                "Cluster %d: %d members",
                cluster_id,
                len(cluster_members_indices),
            )
    else:
        logger.warning("Affinity Propagation did not converge or found no clusters.")
        # If af object is available here, we could log af.n_iter_
        # Consider passing 'af' to this function if n_iter_ is important.


def main() -> None:
    """Run clustering algorithm and update MongoDB records."""
    try:
        setup_logging(__file__, log_directory="logs")
        args = _parse_args()
        _, temp_image_file = _load_and_validate_env_vars()

        client, collection = get_mongodb_connection()
        try:
            coords_df, coords = _load_data(temp_image_file)
            af = _perform_clustering(coords, args.method)

            labels = af.labels_
            cluster_centers_indices = af.cluster_centers_indices_

            # Log clustering metrics
            n_clusters = (
                len(cluster_centers_indices)
                if cluster_centers_indices is not None
                else 0
            )
            logger.info("Estimated number of clusters: %d", n_clusters)
            logger.info("Homogeneity: %.3f", metrics.homogeneity_score(labels, labels))
            logger.info(
                "Completeness: %.3f",
                metrics.completeness_score(labels, labels),
            )
            logger.info("V-measure: %.3f", metrics.v_measure_score(labels, labels))
            logger.info(
                "Adjusted Rand Index: %.3f",
                metrics.adjusted_rand_score(labels, labels),
            )

            _update_database(collection, coords_df, labels, cluster_centers_indices)
            if hasattr(af, "n_iter_") and not (
                cluster_centers_indices is not None
                and len(cluster_centers_indices) > 0
            ) and not (hasattr(labels, "size") and np.unique(labels).size > 0):
                logger.info("Number of iterations: %d", af.n_iter_)

            logger.info("Clustering completed successfully")
        finally:
            client.close()
    except Exception:
        logger.exception("Error during processing")
        sys.exit(1)


if __name__ == "__main__":
    main()
