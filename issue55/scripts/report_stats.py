#!/usr/bin/env python3
"""Extract statistics for the Issue #55 report. Outputs JSON."""

import json
import pandas as pd
import numpy as np
from pathlib import Path

out = {}

# --- Metadata stats ---
meta = pd.read_parquet("../../data/geo_metadata.parquet")
out["total_projects"] = len(meta)
out["n_columns"] = len(meta.columns)

meta["submission_date"] = pd.to_datetime(meta["series_submission_date"], format="%b %d %Y", errors="coerce")
meta["year"] = meta["submission_date"].dt.year
out["date_min"] = str(meta["submission_date"].min().date())
out["date_max"] = str(meta["submission_date"].max().date())
out["unique_series_types"] = meta["series_type"].nunique()
out["unique_organisms"] = meta["series_organism"].nunique()
out["unique_countries"] = meta["series_contact_country"].nunique()
out["unique_institutions"] = meta["series_contact_institute"].nunique()

# Top 5 organisms
org = meta["series_organism"].value_counts().head(5)
out["top5_organisms"] = [{"name": k, "count": int(v)} for k, v in org.items()]

# Top 5 countries
ctry = meta.loc[meta["series_contact_country"].str.len() > 0, "series_contact_country"].value_counts().head(5)
out["top5_countries"] = [{"name": k, "count": int(v)} for k, v in ctry.items()]

# Top 5 institutions
inst = meta.loc[meta["series_contact_institute"].str.len() > 0, "series_contact_institute"].value_counts().head(5)
out["top5_institutions"] = [{"name": k, "count": int(v)} for k, v in inst.items()]

# Top 3 series types
st = meta["series_type"].value_counts().head(3)
out["top3_series_types"] = [{"name": k, "count": int(v)} for k, v in st.items()]

# Year with most projects
yc = meta["year"].value_counts().sort_index()
out["peak_year"] = int(yc.idxmax())
out["peak_year_count"] = int(yc.max())

# --- Text stats ---
text = pd.read_parquet("../../data/geo_text_for_embedding.parquet")
lengths = text["text"].str.len()
out["text_rows"] = len(text)
out["text_median_chars"] = int(lengths.median())
out["text_95th_chars"] = int(lengths.quantile(0.95))
out["text_median_tokens_approx"] = int(lengths.median() / 4)

# --- Embedding stats ---
emb = pd.read_parquet("../../data/geo_embeddings.parquet")
emb_cols = [c for c in emb.columns if c.startswith("emb_")]
out["embedding_rows"] = len(emb)
out["embedding_dims"] = len(emb_cols)

# --- Cluster stats ---
clusters = pd.read_parquet("../output/umap_clusters.parquet")
out["hdbscan_n_clusters"] = len(set(clusters["hdbscan_label"])) - (1 if -1 in clusters["hdbscan_label"].values else 0)
out["hdbscan_noise"] = int((clusters["hdbscan_label"] == -1).sum())
out["hdbscan_noise_pct"] = round((clusters["hdbscan_label"] == -1).mean() * 100, 1)

# K-means 50 stats
k50 = clusters["kmeans_50"].value_counts()
out["kmeans50_min"] = int(k50.min())
out["kmeans50_max"] = int(k50.max())
out["kmeans50_median"] = int(k50.median())

# --- Cluster labels (k=50) ---
labels = pd.read_csv("../output/cluster_labels_k50.csv")
# Merge with metadata for year analysis
df = clusters.merge(meta[["gse_id", "series_submission_date"]], on="gse_id")
df["submission_date"] = pd.to_datetime(df["series_submission_date"], format="%b %d %Y", errors="coerce")
df["year"] = df["submission_date"].dt.year

cluster_year = df.groupby("kmeans_50")["year"].mean()
labels["mean_year"] = labels["cluster"].map(cluster_year)
labels = labels.sort_values("n_projects", ascending=False)

# Top 10 largest clusters
out["top10_clusters"] = []
for _, row in labels.head(10).iterrows():
    out["top10_clusters"].append({
        "cluster": int(row["cluster"]),
        "n": int(row["n_projects"]),
        "label": row["label"],
        "mean_year": round(row["mean_year"], 1),
    })

# 5 newest clusters (exclude "www/genome gov" noise)
clean = labels[~labels["label"].str.contains("www")]
newest = clean.sort_values("mean_year", ascending=False).head(5)
out["newest_clusters"] = []
for _, row in newest.iterrows():
    out["newest_clusters"].append({
        "cluster": int(row["cluster"]),
        "n": int(row["n_projects"]),
        "label": row["label"],
        "mean_year": round(row["mean_year"], 1),
    })

# 5 oldest clusters (exclude "www/genome gov" noise)
oldest = clean.sort_values("mean_year", ascending=True).head(5)
out["oldest_clusters"] = []
for _, row in oldest.iterrows():
    out["oldest_clusters"].append({
        "cluster": int(row["cluster"]),
        "n": int(row["n_projects"]),
        "label": row["label"],
        "mean_year": round(row["mean_year"], 1),
    })

# Projects per year for inline reference
yearly = meta["year"].value_counts().sort_index()
out["projects_2001"] = int(yearly.get(2001, 0))
out["projects_2021"] = int(yearly.get(2021, 0))
out["projects_2024"] = int(yearly.get(2024, 0))

# USA vs China
usa = int(meta[meta["series_contact_country"] == "USA"].shape[0])
china = int(meta[meta["series_contact_country"] == "China"].shape[0])
out["usa_projects"] = usa
out["china_projects"] = china
out["usa_pct"] = round(usa / len(meta) * 100, 1)
out["china_pct"] = round(china / len(meta) * 100, 1)

print(json.dumps(out, indent=2))
