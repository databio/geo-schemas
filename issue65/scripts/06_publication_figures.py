#!/usr/bin/env python3
"""Step 4: Publication figures for schema diversity analysis.
Issue #65 — PEPhub Schema Diversity Analysis

Generates four figures:
  1. Long-tail distribution: column usage frequency (log scale)
  2. Synonym cluster bar chart: top concepts and their variant counts
  3. Schema complexity histogram: columns-per-PEP distribution
  4. Harmonization gap: for top concepts, standard name vs variant usage
"""

import json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

DATA_DIR = Path("../../data")
OUTPUT_DIR = Path("../output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.labelsize": 12,
    "figure.dpi": 150,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.15,
})

# Load data
freq = pd.read_parquet(DATA_DIR / "column_frequencies.parquet")
classifications = pd.read_parquet(DATA_DIR / "column_classifications.parquet")
user_cols = pd.read_parquet(DATA_DIR / "user_defined_columns.parquet")
with open(DATA_DIR / "column_synonyms_full.json") as f:
    synonyms = json.load(f)

# Load original parquet for columns-per-project
import pyarrow.parquet as pq
meta = pq.read_table(DATA_DIR / "geo_metadata.parquet", columns=["n_columns"]).to_pandas()

N_PROJECTS = 229_101

# ============================================================
# Figure 1: Long-tail distribution (column frequency, log scale)
# ============================================================
print("Generating Figure 1: Long-tail distribution...")

fig, ax = plt.subplots(figsize=(10, 5))

# Rank all columns by frequency
user_freq = user_cols.sort_values("n_projects", ascending=False).reset_index(drop=True)
user_freq["rank"] = range(1, len(user_freq) + 1)

ax.plot(user_freq["rank"], user_freq["n_projects"], color="steelblue", linewidth=1.2)
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("Column name rank")
ax.set_ylabel("Number of projects")
ax.set_title("Long-tail distribution of user-defined column names across 229K GEO PEPs")

# Annotate key points
top_labels = [
    ("tissue", 1),
    ("cell_type", 3),
    ("treatment", 4),
    ("age", 5),
    ("sex", 11),
]
for label, rank_approx in top_labels:
    row = user_freq[user_freq["column_name"] == label]
    if not row.empty:
        r = row.iloc[0]
        ax.annotate(label, (r["rank"], r["n_projects"]),
                    textcoords="offset points", xytext=(12, 5),
                    fontsize=9, color="darkred",
                    arrowprops=dict(arrowstyle="-", color="gray", lw=0.5))

# Mark thresholds
ax.axhline(y=N_PROJECTS * 0.5, color="gray", linestyle="--", alpha=0.5, linewidth=0.8)
ax.text(len(user_freq) * 0.7, N_PROJECTS * 0.55, ">50% of projects",
        color="gray", fontsize=8)

# Add singleton count
n_singletons = (user_freq["n_projects"] == 1).sum()
ax.text(0.95, 0.95, f"{n_singletons:,} singletons ({n_singletons/len(user_freq)*100:.0f}%)",
        transform=ax.transAxes, ha="right", va="top", fontsize=9,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", alpha=0.8))

ax.grid(True, alpha=0.3)
fig.savefig(OUTPUT_DIR / "longtail_distribution.png")
plt.close()
print(f"  Saved {OUTPUT_DIR / 'longtail_distribution.png'}")

# ============================================================
# Figure 2: Synonym cluster bar chart
# ============================================================
print("Generating Figure 2: Synonym clusters...")

# Deduplicate clusters — pick the entry with most total_projects for each canonical
# (handles seeds that appear in multiple clusters)
seen_canonicals = set()
deduped = {}
for canonical, data in sorted(synonyms.items(), key=lambda x: -x[1]["total_projects"]):
    if canonical not in seen_canonicals:
        deduped[canonical] = data
        seen_canonicals.add(canonical)

# Top 20 clusters by total projects
top_clusters = list(deduped.items())[:20]

fig, ax = plt.subplots(figsize=(10, 7))

names = [k for k, _ in top_clusters]
n_variants = [len(v["members"]) for _, v in top_clusters]
totals = [v["total_projects"] for _, v in top_clusters]

y = range(len(names))
bars = ax.barh(y, n_variants, color="steelblue", edgecolor="white", linewidth=0.5)

ax.set_yticks(y)
ax.set_yticklabels(names)
ax.invert_yaxis()
ax.set_xlabel("Number of variant column names")
ax.set_title("Schema synonyms: top 20 biological concepts\nand their variant column names across GEO PEPs")

# Add total project counts as text
for i, (nv, total) in enumerate(zip(n_variants, totals)):
    ax.text(nv + 0.3, i, f"{total:,} projects", va="center", fontsize=8, color="gray")

ax.grid(True, axis="x", alpha=0.3)
fig.savefig(OUTPUT_DIR / "synonym_clusters_top20.png")
plt.close()
print(f"  Saved {OUTPUT_DIR / 'synonym_clusters_top20.png'}")

# ============================================================
# Figure 3: Schema complexity histogram
# ============================================================
print("Generating Figure 3: Schema complexity histogram...")

fig, ax = plt.subplots(figsize=(10, 5))

# Clip at 100 for visualization (very few projects have more)
cols_clipped = meta["n_columns"].clip(upper=100)
ax.hist(cols_clipped, bins=70, color="steelblue", edgecolor="white", linewidth=0.3)
ax.set_xlabel("Number of columns per project")
ax.set_ylabel("Number of projects")
ax.set_title("Schema complexity: columns per GEO PEP sample table")

median_val = meta["n_columns"].median()
ax.axvline(x=median_val, color="red", linestyle="--", linewidth=1.2, alpha=0.7)
ax.text(median_val + 1, ax.get_ylim()[1] * 0.9, f"median = {int(median_val)}",
        color="red", fontsize=10)

ax.grid(True, alpha=0.3)
fig.savefig(OUTPUT_DIR / "schema_complexity_histogram.png")
plt.close()
print(f"  Saved {OUTPUT_DIR / 'schema_complexity_histogram.png'}")

# ============================================================
# Figure 4: Harmonization gap — standard name vs variants
# ============================================================
print("Generating Figure 4: Harmonization gap...")

# For top 10 concepts: what fraction uses the canonical name vs variants?
top10 = list(deduped.items())[:10]

fig, ax = plt.subplots(figsize=(10, 6))

concepts = []
canonical_pcts = []
variant_pcts = []

for canonical, data in top10:
    members = data["members"]
    total = data["total_projects"]
    canonical_count = members.get(canonical, 0)
    variant_count = total - canonical_count

    concepts.append(canonical)
    canonical_pcts.append(canonical_count / total * 100)
    variant_pcts.append(variant_count / total * 100)

y = range(len(concepts))
ax.barh(y, canonical_pcts, color="steelblue", label="Canonical name", edgecolor="white")
ax.barh(y, variant_pcts, left=canonical_pcts, color="coral", label="Variant names",
        edgecolor="white")

ax.set_yticks(y)
ax.set_yticklabels(concepts)
ax.invert_yaxis()
ax.set_xlabel("Percentage of projects")
ax.set_title("Harmonization gap: canonical name vs variant usage\nfor top 10 biological concepts in GEO")
ax.legend(loc="lower right")

# Add percentage labels
for i, (cp, vp) in enumerate(zip(canonical_pcts, variant_pcts)):
    if cp > 8:
        ax.text(cp / 2, i, f"{cp:.0f}%", ha="center", va="center", fontsize=8, color="white")
    if vp > 8:
        ax.text(cp + vp / 2, i, f"{vp:.0f}%", ha="center", va="center", fontsize=8, color="white")

ax.set_xlim(0, 100)
ax.grid(True, axis="x", alpha=0.3)
fig.savefig(OUTPUT_DIR / "harmonization_gap.png")
plt.close()
print(f"  Saved {OUTPUT_DIR / 'harmonization_gap.png'}")

print(f"\nStep 4 complete. All figures in: {OUTPUT_DIR}")
