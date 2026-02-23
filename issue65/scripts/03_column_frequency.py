#!/usr/bin/env python3
"""Step 1: Column frequency analysis of GEO sample table schemas.
Issue #65 — PEPhub Schema Diversity Analysis

Explodes column_names_json to one row per column per project, counts
frequency of each unique column name, and characterizes the distribution.
"""

import json
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from collections import Counter
from pathlib import Path

DATA_DIR = Path("../../data")
OUTPUT_DIR = Path("../output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILE = DATA_DIR / "geo_metadata.parquet"
FREQ_OUTPUT = DATA_DIR / "column_frequencies.parquet"

# Load data
print("Loading geo_metadata.parquet...")
df = pq.read_table(INPUT_FILE, columns=["gse_id", "n_columns", "column_names_json"]).to_pandas()
print(f"Loaded {len(df)} projects")

# Parse JSON column names
print("Parsing column_names_json...")
df["columns"] = df["column_names_json"].apply(json.loads)

# Explode to one row per column per project
print("Exploding to per-column rows...")
exploded = df[["gse_id", "columns"]].explode("columns").rename(columns={"columns": "column_name"})
exploded = exploded.dropna(subset=["column_name"])
print(f"Total column-project pairs: {len(exploded):,}")

# Count frequency of each column name
print("Counting column frequencies...")
col_counts = exploded["column_name"].value_counts().reset_index()
col_counts.columns = ["column_name", "n_projects"]
col_counts["pct_projects"] = (col_counts["n_projects"] / len(df) * 100).round(4)
print(f"Unique column names: {len(col_counts):,}")

# Save frequencies
col_counts.to_parquet(FREQ_OUTPUT, index=False)
print(f"Saved {FREQ_OUTPUT}")

# Also save as CSV for easy inspection
col_counts.to_csv(OUTPUT_DIR / "column_frequencies.csv", index=False)

# ============================================================
# Distribution summary
# ============================================================
print("\n" + "=" * 60)
print("COLUMN FREQUENCY DISTRIBUTION")
print("=" * 60)

n_projects = len(df)
n_unique = len(col_counts)
n_pairs = len(exploded)

print(f"Total projects:               {n_projects:,}")
print(f"Total column-project pairs:   {n_pairs:,}")
print(f"Unique column names:          {n_unique:,}")
print(f"Mean columns per project:     {n_pairs / n_projects:.1f}")
print(f"Median columns per project:   {df['n_columns'].median():.0f}")

# Frequency buckets
universal = col_counts[col_counts["pct_projects"] >= 90]
common = col_counts[(col_counts["pct_projects"] >= 10) & (col_counts["pct_projects"] < 90)]
uncommon = col_counts[(col_counts["pct_projects"] >= 1) & (col_counts["pct_projects"] < 10)]
rare = col_counts[(col_counts["pct_projects"] >= 0.1) & (col_counts["pct_projects"] < 1)]
singletons = col_counts[col_counts["n_projects"] == 1]

print(f"\n--- Frequency Buckets ---")
print(f"Universal (>=90%):   {len(universal):>6,}  ({len(universal)/n_unique*100:.2f}%)")
print(f"Common (10-90%):     {len(common):>6,}  ({len(common)/n_unique*100:.2f}%)")
print(f"Uncommon (1-10%):    {len(uncommon):>6,}  ({len(uncommon)/n_unique*100:.2f}%)")
print(f"Rare (0.1-1%):       {len(rare):>6,}  ({len(rare)/n_unique*100:.2f}%)")
print(f"Very rare (<0.1%):   {len(col_counts[col_counts['pct_projects'] < 0.1]):>6,}")
print(f"Singletons (n=1):    {len(singletons):>6,}  ({len(singletons)/n_unique*100:.2f}%)")

# Top 50 columns
print(f"\n--- Top 50 Most Common Columns ---")
for _, row in col_counts.head(50).iterrows():
    print(f"  {row['column_name']:<45s}  {row['n_projects']:>7,}  ({row['pct_projects']:>6.2f}%)")

# Columns-per-project distribution
print(f"\n--- Columns per Project ---")
cpd = df["n_columns"]
print(f"  Min:    {cpd.min()}")
print(f"  25th:   {int(cpd.quantile(0.25))}")
print(f"  Median: {int(cpd.median())}")
print(f"  75th:   {int(cpd.quantile(0.75))}")
print(f"  95th:   {int(cpd.quantile(0.95))}")
print(f"  Max:    {cpd.max()}")

# Save summary stats
summary = {
    "n_projects": int(n_projects),
    "n_unique_columns": int(n_unique),
    "n_column_project_pairs": int(n_pairs),
    "mean_columns_per_project": round(n_pairs / n_projects, 1),
    "median_columns_per_project": int(cpd.median()),
    "n_universal_gte90pct": int(len(universal)),
    "n_common_10_90pct": int(len(common)),
    "n_uncommon_1_10pct": int(len(uncommon)),
    "n_rare_01_1pct": int(len(rare)),
    "n_singletons": int(len(singletons)),
}
pd.Series(summary).to_json(OUTPUT_DIR / "frequency_summary.json", indent=2)

print(f"\nStep 1 complete. Output in: {OUTPUT_DIR}")
