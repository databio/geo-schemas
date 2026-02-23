#!/usr/bin/env python3
"""Step 6: Temporal and institutional analysis.
Issue #55 — GEO Series-Level Semantic Meta Analysis

Pseudotime plots, institutional profiles, regional analysis,
cluster sparsity, and modality trends.
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime

OUTPUT_DIR = Path("../output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Load data ---
print("Loading data...")
clusters = pd.read_parquet(OUTPUT_DIR / "umap_clusters.parquet")
meta = pd.read_parquet("../../data/geo_metadata.parquet")
labels = pd.read_csv(OUTPUT_DIR / "cluster_labels_k50.csv")

df = clusters.merge(meta, on="gse_id")

# Parse dates
df["submission_date"] = pd.to_datetime(df["series_submission_date"], format="%b %d %Y", errors="coerce")
df["year"] = df["submission_date"].dt.year

# Create cluster label map
label_map = dict(zip(labels["cluster"], labels["label"]))

CLUSTER_COL = "kmeans_50"

# ============================================================
# 1. Pseudotime: color UMAP by submission year
# ============================================================
print("Plotting pseudotime UMAP...")
fig, ax = plt.subplots(figsize=(12, 10))
valid = df["year"].notna()
sc = ax.scatter(
    df.loc[valid, "umap_1"], df.loc[valid, "umap_2"],
    c=df.loc[valid, "year"], cmap="viridis", s=0.3, alpha=0.4,
)
plt.colorbar(sc, ax=ax, label="Submission Year")
ax.set_title("GEO Projects: UMAP colored by submission year")
ax.set_xlabel("UMAP 1")
ax.set_ylabel("UMAP 2")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "umap_pseudotime.png", dpi=200)
plt.close()
print(f"  Saved umap_pseudotime.png")

# ============================================================
# 2. Cluster composition over time
# ============================================================
print("Analyzing cluster composition over time...")
yearly_cluster = df.groupby(["year", CLUSTER_COL]).size().unstack(fill_value=0)
yearly_pct = yearly_cluster.div(yearly_cluster.sum(axis=1), axis=0)

# Pick top 10 clusters by size for readability
top_clusters = df[CLUSTER_COL].value_counts().head(10).index
yearly_pct_top = yearly_pct[top_clusters]

fig, ax = plt.subplots(figsize=(14, 7))
yearly_pct_top.plot.area(ax=ax, alpha=0.7)
ax.set_title("Top 10 Clusters: Proportion Over Time")
ax.set_xlabel("Submission Year")
ax.set_ylabel("Proportion of Projects")
ax.legend(
    [f"C{c}: {label_map.get(c, '?')[:30]}" for c in top_clusters],
    bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=8,
)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "cluster_trends_top10.png", dpi=150)
plt.close()
print(f"  Saved cluster_trends_top10.png")

# ============================================================
# 3. Country profiles on UMAP
# ============================================================
print("Plotting country profiles...")
top_countries = ["USA", "China", "Germany", "Japan", "United Kingdom"]
fig, axes = plt.subplots(1, len(top_countries), figsize=(25, 5))
for ax, country in zip(axes, top_countries):
    mask = df["series_contact_country"] == country
    ax.scatter(df["umap_1"], df["umap_2"], s=0.1, c="lightgrey", alpha=0.1)
    ax.scatter(df.loc[mask, "umap_1"], df.loc[mask, "umap_2"],
               s=0.3, c="red", alpha=0.3)
    ax.set_title(f"{country} (n={mask.sum()})")
    ax.set_xticks([])
    ax.set_yticks([])
plt.suptitle("Country Profiles on UMAP", fontsize=14)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "umap_countries.png", dpi=150)
plt.close()
print(f"  Saved umap_countries.png")

# ============================================================
# 4. Institutional profiles: top 10 institutions on UMAP
# ============================================================
print("Plotting institutional profiles...")
top_insts = df["series_contact_institute"].value_counts().head(10).index.tolist()
fig, axes = plt.subplots(2, 5, figsize=(25, 10))
for ax, inst in zip(axes.flat, top_insts):
    mask = df["series_contact_institute"] == inst
    ax.scatter(df["umap_1"], df["umap_2"], s=0.1, c="lightgrey", alpha=0.1)
    ax.scatter(df.loc[mask, "umap_1"], df.loc[mask, "umap_2"],
               s=0.5, c="blue", alpha=0.4)
    short_name = inst[:25] + "..." if len(inst) > 25 else inst
    ax.set_title(f"{short_name} (n={mask.sum()})", fontsize=9)
    ax.set_xticks([])
    ax.set_yticks([])
plt.suptitle("Top 10 Institutions on UMAP", fontsize=14)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "umap_institutions.png", dpi=150)
plt.close()
print(f"  Saved umap_institutions.png")

# ============================================================
# 5. Modality trends over time
# ============================================================
print("Analyzing modality trends...")
# Simplify series_type to major categories
def simplify_type(t):
    if pd.isna(t):
        return "Other"
    t = str(t).lower()
    if "expression" in t and "high throughput" in t:
        return "RNA-seq"
    elif "expression" in t and "array" in t:
        return "Microarray"
    elif "binding" in t or "chip" in t:
        return "ChIP-seq"
    elif "methylation" in t:
        return "Methylation"
    elif "non-coding" in t:
        return "ncRNA"
    elif "variation" in t or "snp" in t:
        return "Genomic variation"
    else:
        return "Other"

df["modality"] = df["series_type"].apply(simplify_type)
mod_yearly = df.groupby(["year", "modality"]).size().unstack(fill_value=0)

fig, ax = plt.subplots(figsize=(12, 6))
mod_yearly.plot(ax=ax, linewidth=2)
ax.set_title("GEO Modality Trends Over Time")
ax.set_xlabel("Submission Year")
ax.set_ylabel("Number of Projects")
ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "modality_trends.png", dpi=150)
plt.close()
print(f"  Saved modality_trends.png")

# ============================================================
# 6. Cluster sparsity: identify under-researched topics
# ============================================================
print("Analyzing cluster sparsity...")
cluster_stats = df.groupby(CLUSTER_COL).agg(
    n_projects=("gse_id", "count"),
    mean_year=("year", "mean"),
    median_year=("year", "median"),
    umap1_std=("umap_1", "std"),
    umap2_std=("umap_2", "std"),
).reset_index()
cluster_stats["spread"] = np.sqrt(cluster_stats["umap1_std"]**2 + cluster_stats["umap2_std"]**2)
cluster_stats["label"] = cluster_stats[CLUSTER_COL].map(label_map)
cluster_stats = cluster_stats.sort_values("n_projects")

cluster_stats.to_csv(OUTPUT_DIR / "cluster_stats.csv", index=False)
print(f"  Saved cluster_stats.csv")

# Smallest clusters = potentially under-researched
print("\n--- Smallest clusters (potential under-researched topics) ---")
print(cluster_stats.head(10)[["kmeans_50", "n_projects", "mean_year", "label"]].to_string(index=False))

# Newest clusters (high mean year)
print("\n--- Newest clusters (emerging topics) ---")
newest = cluster_stats.sort_values("mean_year", ascending=False).head(10)
print(newest[["kmeans_50", "n_projects", "mean_year", "label"]].to_string(index=False))

print("\nStep 6 complete.")
