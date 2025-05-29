"""Runs the clustering algorithm.

Reads the CSV file and runs the clustering algorithm. Currently Affinity Propagation.
Updates each media element record in the MongoDB collection with the cluster
information.

Step 2.
"""

import argparse
import os
import sys
from pathlib import Path

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
        raise ValueError(f"Unknown distance calculation method: {method}")

    return distance_matrix


def main() -> None:
    """Run clustering algorithm and update MongoDB records."""
    try:
        # Setup logging
        setup_logging(__file__, log_directory="logs")

        # Load environment variables
        load_dotenv()

        # Parse command line arguments
        parser = argparse.ArgumentParser(
            description="Photo clustering with different distance calculation methods",
        )
        parser.add_argument(
            "--method",
            choices=["geodesic", "haversine"],
            default="haversine",
            help="Distance calculation method to use",
        )
        args = parser.parse_args()

        # Validate environment variables
        database = os.getenv("MONGO_DATABASE")
        temp_image_file = Path(os.getenv("TEMP_IMAGE_FILE", ""))

        if not database:
            raise ValueError("MONGO_DATABASE environment variable is required")
        if not temp_image_file or not temp_image_file.is_file():
            raise FileNotFoundError(
                "Environment variable TEMP_IMAGE_FILE is not set or points to a non-existent file.",
            )

        # Connect to MongoDB
        client, collection = get_mongodb_connection()

        try:
            # Read coordinates from CSV
            df = pd.read_csv(temp_image_file)
            coords = df[df.columns[1:3]].values
            logger.info("Loaded %d coordinates from CSV", len(coords))

            # Calculate distances
            logger.info("Using %s distance calculation method", args.method)
            distance_matrix = calculate_distances(coords, method=args.method)
            similarity_matrix = -distance_matrix

            # Run Affinity Propagation
            af = AffinityPropagation(
                affinity="precomputed",
                damping=0.9,
                max_iter=500,
                random_state=42,
            )
            af.fit(similarity_matrix)

            cluster_centers_indices = af.cluster_centers_indices_
            n_clusters = (
                len(cluster_centers_indices)
                if cluster_centers_indices is not None
                else 0
            )
            labels = af.labels_

            # Log clustering metrics
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

            # Add cluster labels to DataFrame
            df["cluster_id"] = labels
            centers = []

            if cluster_centers_indices is not None and len(cluster_centers_indices) > 0:
                for cluster_id in range(n_clusters):
                    center_index = cluster_centers_indices[cluster_id]
                    center_point_details = df.iloc[center_index]

                    # Update all photos in this cluster
                    cluster_members_indices = np.where(labels == cluster_id)[0]
                    member_source_files = [
                        df.iloc[idx]["SourceFile"] for idx in cluster_members_indices
                    ]

                    # Update all non-center members
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

                    # Update the center photo
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

                    centers.append(
                        {
                            "latitude": float(center_point_details["GPSLatitude"]),
                            "longitude": float(center_point_details["GPSLongitude"]),
                            "cluster_id": cluster_id,
                        },
                    )

            elif hasattr(af, "labels_") and len(np.unique(af.labels_)) > 0:
                labels = af.labels_
                unique_labels = np.unique(labels)
                n_clusters = len(unique_labels)
                logger.warning(
                    "Affinity Propagation converged, but cluster centers might be ambiguous.",
                )
                logger.info(
                    "Estimated number of clusters based on labels: %d",
                    n_clusters,
                )

                for cluster_id in unique_labels:
                    cluster_members_indices = np.where(labels == cluster_id)[0]
                    cluster_members_details = df.iloc[cluster_members_indices]
                    logger.info(
                        "Cluster %d: %d members",
                        cluster_id,
                        len(cluster_members_indices),
                    )

            else:
                logger.warning(
                    "Affinity Propagation did not converge or found no clusters.",
                )
                if hasattr(af, "n_iter_"):
                    logger.info("Number of iterations: %d", af.n_iter_)

            logger.info("Clustering completed successfully")

        finally:
            client.close()

    except Exception:
        logger.exception("Error during processing")
        sys.exit(1)


if __name__ == "__main__":
    main()
