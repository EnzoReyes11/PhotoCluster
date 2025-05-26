"""Photo Clusters - Affinity Propagation."""

import argparse
import os

import numpy as np
import pandas as pd
import pymongo
from dotenv import load_dotenv
from geopy.distance import geodesic
from haversine import Unit, haversine
from sklearn import metrics
from sklearn.cluster import AffinityPropagation
from sklearn.metrics.pairwise import pairwise_distances

load_dotenv()

MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "photoLocator")

myclient = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
db = myclient[MONGO_DATABASE]
photos = db["photos"]

query = {"$and": [{"GPSPosition": {"$ne": None}}, {"GPSAltitude": {"$ne": None}}]}
datapoints = list(photos.find(query))

# df = pd.json_normalize(datapoints)
TEMP_IMAGE_FILE = os.getenv("TEMP_IMAGE_FILE")
if not TEMP_IMAGE_FILE or not os.path.isfile(TEMP_IMAGE_FILE):
    raise FileNotFoundError(
        "Environment variable TEMP_IMAGE_FILE is not set or points to a "
        "non‑existent file.",
    )

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

df = pd.read_csv(TEMP_IMAGE_FILE)

df[df.columns[1:3]]

coords = df[df.columns[1:3]].values
print(coords)


def calculate_distances(coords, method="haversine"):
    """Calculate distances between points using the specified method"""
    if method == "geodesic":
        print("Calculating pairwise Geodesic distances...")

        def geodesic_wrapper(point1, point2):
            return geodesic(point1, point2).km

        distance_matrix = pairwise_distances(coords, metric=geodesic_wrapper)
    elif method == "haversine":
        print("Calculating pairwise Haversine distances...")

        def haversine_wrapper(point1, point2):
            # haversine expects (lat, lon) tuples and returns distance in km
            return haversine(point1, point2, unit=Unit.KILOMETERS)

        distance_matrix = pairwise_distances(coords, metric=haversine_wrapper)
    else:
        raise ValueError(f"Unknown distance calculation method: {method}")

    return distance_matrix


print(f"Using {args.method} distance calculation method")
distance_matrix = calculate_distances(coords, method=args.method)

similarity_matrix = -distance_matrix

"""
=================================================
Demo of affinity propagation clustering algorithm
=================================================

Reference:
Brendan J. Frey and Delbert Dueck, "Clustering by Passing Messages
Between Data Points", Science Feb. 2007

"""

X = df[df.columns[1:3]]

# %%
# Compute Affinity Propagation
# ----------------------------
af = AffinityPropagation(
    affinity="precomputed",
    damping=0.9,  # Adjust damping if convergence issues occur
    max_iter=500,
    random_state=42,
)  # For reproducible results
af.fit(similarity_matrix)

cluster_centers_indices = af.cluster_centers_indices_
n_clusters_ = len(cluster_centers_indices) if cluster_centers_indices is not None else 0
labels = af.labels_

# Affinity Propagation metrics
print(f"Estimated number of clusters: {n_clusters_}")
print(f"Homogeneity: {metrics.homogeneity_score(labels, labels):.3f}")
print(f"Completeness: {metrics.completeness_score(labels, labels):.3f}")
print(f"V-measure: {metrics.v_measure_score(labels, labels):.3f}")
print(f"Adjusted Rand Index: {metrics.adjusted_rand_score(labels, labels):.3f}")

# Add cluster labels back to the original DataFrame (optional)
df["cluster_id"] = labels
centers = []

if cluster_centers_indices is not None and len(cluster_centers_indices) > 0:
    n_clusters_ = len(cluster_centers_indices)
    print(f"Estimated number of clusters: {n_clusters_}")

    for cluster_id in range(n_clusters_):
        center_index = cluster_centers_indices[cluster_id]
        center_point_details = df.iloc[center_index]

        # Update all photos in this cluster
        cluster_members_indices = np.where(labels == cluster_id)[0]

        member_source_files = [
            df.iloc[idx]["SourceFile"] for idx in cluster_members_indices
        ]

        # Update all non-center members in a single operation
        if member_source_files:
            update_result = photos.update_many(
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

        # Update the center photo
        photos.update_one(
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

        centers.append(
            {
                "latitude": float(center_point_details["GPSLatitude"]),
                "longitude": float(center_point_details["GPSLongitude"]),
                "cluster_id": cluster_id,
            },
        )

elif hasattr(af, "labels_") and len(np.unique(af.labels_)) > 0:
    # Handle cases where fit converges but cluster_centers_indices_ might be empty/None
    labels = af.labels_
    unique_labels = np.unique(labels)
    n_clusters_ = len(unique_labels)
    print("\nAffinity Propagation converged, but cluster centers might be ambiguous.")
    print(f"Estimated number of clusters based on labels: {n_clusters_}")

    for cluster_id in unique_labels:
        cluster_members_indices = np.where(labels == cluster_id)[0]
        cluster_members_details = df.iloc[cluster_members_indices]
        print(f"\n--- Cluster {cluster_id} ---")
        print(f"  Number of members: {len(cluster_members_indices)}")
        print("  Members (Index -> [Lat, Lon] (Name)):")
        # for index, member in cluster_members_details.iterrows():
        #  print(f"    - Index {index} -> {member[['latitude', 'longitude']].tolist()} (Name: {member.get('location_name', 'N/A')})")

else:
    print("\nAffinity Propagation did not converge or found no clusters.")
    if hasattr(af, "n_iter_"):
        print(f"Number of iterations: {af.n_iter_}")

# print("\nDataFrame with cluster assignments:\n", df)
print("\nScript finished.")

df.plot.scatter(x="GPSLongitude", y="GPSLatitude", c=labels, s=50, cmap="viridis")
# plt.scatter(centers[:, 0], centers[:, 1], c='black', s=200, alpha=0.5)
