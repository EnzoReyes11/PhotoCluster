"""Runs the clustering algorithm.

Reads the CSV file and runs the clustering algorithm. Currently Affinity Propagation.
Updates each media element record in the MongoDB collection with the cluster
information.

Step 2.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from geopy.distance import geodesic
from haversine import Unit, haversine
from sklearn import metrics
from sklearn.cluster import AffinityPropagation
from sklearn.metrics.pairwise import pairwise_distances

from logger import get_logger, setup_logging
from utils.fs_utils import get_validated_path_from_env

load_dotenv()

from db import get_mongodb_connection  # noqa: E402

if TYPE_CHECKING:
    from pathlib import Path

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


# Removed _ensure_mongo_database_env_var_present (now handled by db.py)
# Removed _ensure_temp_image_file_env_var_present (now use get_validated_path_from_env)
# Removed _ensure_file_exists (now use get_validated_path_from_env)


def _load_and_validate_env_vars() -> Path:  # Only returns temp_image_file now
    """Load and validate environment variables using utility functions."""
    try:
        # MONGO_DATABASE validation is handled by db.py via get_mongodb_connection().
        # No need to validate it here if not used for other purposes.
        temp_image_file = get_validated_path_from_env(
            var_name="TEMP_IMAGE_FILE",
            purpose="temporary image file for clustering",
            check_exists=True,
            check_is_file=True,
        )
    except (ValueError, FileNotFoundError):  # Catching errors from the utility
        logger.exception("Environment variable validation failed")
        raise  # Re-raise to be caught by main's try-except

    return temp_image_file  # Return only temp_image_file


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
            n_clusters_from_labels,  # Use the new variable name
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


def _log_cluster_metrics(
    cluster_centers_indices: np.ndarray | None,
    labels: np.ndarray,
) -> None:
    # Log clustering metrics
    n_clusters = (
        len(cluster_centers_indices) if cluster_centers_indices is not None else 0
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


def main() -> None:
    """Run clustering algorithm and update MongoDB records."""
    try:
        setup_logging(__file__, log_directory="logs")
        args = _parse_args()
        # temp_image_file is now the only return value from _load_and_validate_env_vars
        temp_image_file = _load_and_validate_env_vars()

        client, collection = get_mongodb_connection()
        try:
            coords_df, coords = _load_data(temp_image_file)
            af = _perform_clustering(coords, args.method)

            labels = af.labels_
            cluster_centers_indices = af.cluster_centers_indices_

            _log_cluster_metrics(cluster_centers_indices, labels)

            _update_database(collection, coords_df, labels, cluster_centers_indices)
            if (
                hasattr(af, "n_iter_")
                and not (
                    cluster_centers_indices is not None
                    and len(cluster_centers_indices) > 0
                )
                and not (hasattr(labels, "size") and np.unique(labels).size > 0)
            ):
                logger.info("Number of iterations: %d", af.n_iter_)

            logger.info("Clustering completed successfully")
        finally:
            client.close()
    except Exception:
        logger.exception("Error during processing")
        sys.exit(1)


if __name__ == "__main__":
    main()
