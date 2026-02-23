#!/usr/bin/env python3
"""Step 4: UMAP dimensionality reduction and clustering.
Issue #55 — GEO Series-Level Semantic Meta Analysis

Reads embedding vectors, runs UMAP for 2D projection,
then clusters with HDBSCAN and k-means. Saves results.
"""

import time
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import umap
import hdbscan
from sklearn.cluster import MiniBatchKMeans
from pathlib import Path

INPUT_FILE = "../../data/geo_embeddings.parquet"
META_FILE = "../../data/geo_metadata.parquet"
OUTPUT_DIR = Path("../output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Load embeddings ---
print("Loading embeddings...")
df_emb = pd.read_parquet(INPUT_FILE)
gse_ids = df_emb["gse_id"].values
emb_cols = [c for c in df_emb.columns if c.startswith("emb_")]
X = df_emb[emb_cols].values.astype(np.float32)
print(f"Embeddings: {X.shape[0]} x {X.shape[1]}")

# --- UMAP ---
print("Running UMAP (2D)...")
t0 = time.time()
reducer = umap.UMAP(
    n_components=2,
    n_neighbors=30,
    min_dist=0.1,
    metric="cosine",
    random_state=42,
    verbose=True,
)
umap_2d = reducer.fit_transform(X)
print(f"UMAP done in {time.time() - t0:.1f}s")

# --- HDBSCAN ---
print("Running HDBSCAN...")
t0 = time.time()
clusterer = hdbscan.HDBSCAN(
    min_cluster_size=100,
    min_samples=10,
    cluster_selection_method="eom",
    prediction_data=True,
)
hdb_labels = clusterer.fit_predict(umap_2d)
n_hdb = len(set(hdb_labels)) - (1 if -1 in hdb_labels else 0)
n_noise = (hdb_labels == -1).sum()
print(f"HDBSCAN: {n_hdb} clusters, {n_noise} noise points ({time.time() - t0:.1f}s)")

# --- K-means ---
K_VALUES = [20, 50, 100]
kmeans_results = {}
for k in K_VALUES:
    print(f"Running k-means (k={k})...")
    t0 = time.time()
    km = MiniBatchKMeans(n_clusters=k, random_state=42, batch_size=2048)
    km_labels = km.fit_predict(X)
    inertia = km.inertia_
    print(f"  k={k}: inertia={inertia:.0f} ({time.time() - t0:.1f}s)")
    kmeans_results[k] = km_labels

# --- Save results ---
print("Saving results...")
results = pd.DataFrame({
    "gse_id": gse_ids,
    "umap_1": umap_2d[:, 0],
    "umap_2": umap_2d[:, 1],
    "hdbscan_label": hdb_labels,
})
for k in K_VALUES:
    results[f"kmeans_{k}"] = kmeans_results[k]

results.to_parquet(OUTPUT_DIR / "umap_clusters.parquet", index=False)
print(f"Saved {OUTPUT_DIR / 'umap_clusters.parquet'}")

# --- Summary ---
print(f"\n--- Cluster Summary ---")
print(f"HDBSCAN: {n_hdb} clusters, {n_noise} noise ({n_noise/len(gse_ids)*100:.1f}%)")
for k in K_VALUES:
    sizes = pd.Series(kmeans_results[k]).value_counts().sort_index()
    print(f"K-means k={k}: min={sizes.min()}, max={sizes.max()}, median={sizes.median():.0f}")

# --- Quick UMAP plot colored by HDBSCAN ---
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(20, 8))

    # HDBSCAN
    ax = axes[0]
    noise = hdb_labels == -1
    ax.scatter(umap_2d[noise, 0], umap_2d[noise, 1], s=0.5, c="lightgrey", alpha=0.3)
    sc = ax.scatter(umap_2d[~noise, 0], umap_2d[~noise, 1], s=0.5,
                    c=hdb_labels[~noise], cmap="tab20", alpha=0.5)
    ax.set_title(f"HDBSCAN ({n_hdb} clusters)")
    ax.set_xlabel("UMAP 1")
    ax.set_ylabel("UMAP 2")

    # K-means 50
    ax = axes[1]
    sc = ax.scatter(umap_2d[:, 0], umap_2d[:, 1], s=0.5,
                    c=kmeans_results[50], cmap="tab20", alpha=0.5)
    ax.set_title("K-means (k=50)")
    ax.set_xlabel("UMAP 1")
    ax.set_ylabel("UMAP 2")

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "umap_clusters.png", dpi=200)
    print(f"Saved {OUTPUT_DIR / 'umap_clusters.png'}")
except Exception as e:
    print(f"Plot failed: {e}")

print("\nStep 4 complete.")
