#!/usr/bin/env python3
"""Step 5: Cluster labeling via keyword extraction.
Issue #55 — GEO Series-Level Semantic Meta Analysis

For each cluster, extracts top TF-IDF terms to create interpretable labels.
Joins with metadata for category-based characterization of clusters.
"""

import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from pathlib import Path
from collections import Counter

OUTPUT_DIR = Path("../output")

# --- Load data ---
print("Loading data...")
clusters = pd.read_parquet(OUTPUT_DIR / "umap_clusters.parquet")
texts = pd.read_parquet("../../data/geo_text_for_embedding.parquet")
meta = pd.read_parquet("../../data/geo_metadata.parquet",
                        columns=["gse_id", "series_type", "series_organism",
                                 "series_contact_country", "series_contact_institute",
                                 "series_submission_date"])

df = clusters.merge(texts, on="gse_id").merge(meta, on="gse_id")
print(f"Loaded {len(df)} rows")

# Use kmeans_50 as primary clustering (good granularity)
CLUSTER_COL = "kmeans_50"

# --- TF-IDF keyword extraction per cluster ---
print(f"\nExtracting keywords per cluster ({CLUSTER_COL})...")
vectorizer = TfidfVectorizer(
    max_features=10000,
    stop_words="english",
    max_df=0.5,
    min_df=5,
    ngram_range=(1, 2),
)
tfidf_matrix = vectorizer.fit_transform(df["text"])
feature_names = vectorizer.get_feature_names_out()

cluster_labels = {}
rows = []
for cluster_id in sorted(df[CLUSTER_COL].unique()):
    mask = df[CLUSTER_COL] == cluster_id
    n = mask.sum()

    # Mean TF-IDF for cluster
    cluster_tfidf = tfidf_matrix[mask.values].mean(axis=0)
    cluster_tfidf = np.asarray(cluster_tfidf).flatten()

    top_idx = cluster_tfidf.argsort()[-10:][::-1]
    top_terms = [(feature_names[i], cluster_tfidf[i]) for i in top_idx]

    # Top organisms and series types
    top_organism = df.loc[mask, "series_organism"].value_counts().head(3)
    top_type = df.loc[mask, "series_type"].value_counts().head(3)
    top_country = df.loc[mask, "series_contact_country"].value_counts().head(3)

    label = " / ".join([t[0] for t in top_terms[:3]])
    cluster_labels[cluster_id] = label

    rows.append({
        "cluster": cluster_id,
        "n_projects": n,
        "label": label,
        "top_keywords": ", ".join([f"{t[0]} ({t[1]:.3f})" for t in top_terms]),
        "top_organisms": ", ".join([f"{o} ({c})" for o, c in top_organism.items()]),
        "top_types": ", ".join([f"{t} ({c})" for t, c in top_type.items()]),
        "top_countries": ", ".join([f"{c} ({n})" for c, n in top_country.items()]),
    })

labels_df = pd.DataFrame(rows)
labels_df.to_csv(OUTPUT_DIR / "cluster_labels_k50.csv", index=False)
print(f"Saved {OUTPUT_DIR / 'cluster_labels_k50.csv'}")

# Print summary
print(f"\n--- Cluster Labels (k=50) ---")
for _, row in labels_df.iterrows():
    print(f"  Cluster {row['cluster']:2d} (n={row['n_projects']:5d}): {row['label']}")

# --- Also label HDBSCAN clusters ---
print(f"\n--- HDBSCAN cluster labels ---")
hdb_rows = []
for cluster_id in sorted(df["hdbscan_label"].unique()):
    if cluster_id == -1:
        continue
    mask = df["hdbscan_label"] == cluster_id
    n = mask.sum()
    cluster_tfidf = tfidf_matrix[mask.values].mean(axis=0)
    cluster_tfidf = np.asarray(cluster_tfidf).flatten()
    top_idx = cluster_tfidf.argsort()[-5:][::-1]
    top_terms = [feature_names[i] for i in top_idx]
    label = " / ".join(top_terms[:3])
    print(f"  Cluster {cluster_id:2d} (n={n:5d}): {label}")
    hdb_rows.append({"cluster": cluster_id, "n_projects": n, "label": label})

hdb_labels_df = pd.DataFrame(hdb_rows)
hdb_labels_df.to_csv(OUTPUT_DIR / "cluster_labels_hdbscan.csv", index=False)

print("\nStep 5 complete.")
