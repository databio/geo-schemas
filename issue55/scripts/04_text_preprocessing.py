#!/usr/bin/env python3
"""Step 2: Text preprocessing for embedding generation.
Issue #55 — GEO Series-Level Semantic Meta Analysis

Concatenates series_title + series_summary + series_overall_design into a
single text field, cleans whitespace/encoding, evaluates text length
distribution, and writes geo_text_for_embedding.parquet.
"""

import re
import pyarrow.parquet as pq
import pandas as pd
import numpy as np

DATA_DIR = "../../data"
INPUT_FILE = f"{DATA_DIR}/geo_metadata.parquet"
OUTPUT_FILE = f"{DATA_DIR}/geo_text_for_embedding.parquet"

print("Loading geo_metadata.parquet...")
df = pq.read_table(INPUT_FILE).to_pandas()
print(f"Loaded {len(df)} rows")

# Select text columns for concatenation
text_cols = ["series_title", "series_summary", "series_overall_design"]

def clean_text(s):
    """Clean a text field: handle NaN, collapse whitespace, strip."""
    if pd.isna(s) or s is None:
        return ""
    s = str(s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

# Build combined text
print("Combining text fields...")
parts = []
for col in text_cols:
    parts.append(df[col].apply(clean_text))

# Join with separator, skip empty parts
def combine_row(title, summary, design):
    pieces = [p for p in [title, summary, design] if p]
    return " ".join(pieces)

df["text"] = [combine_row(t, s, d) for t, s, d in zip(parts[0], parts[1], parts[2])]

# Remove rows with empty text
empty_mask = df["text"].str.len() == 0
n_empty = empty_mask.sum()
print(f"Empty text rows: {n_empty}")
df_out = df.loc[~empty_mask, ["gse_id", "text"]].reset_index(drop=True)
print(f"Rows with text: {len(df_out)}")

# Text length statistics
lengths = df_out["text"].str.len()
print(f"\n--- Text Length Distribution ---")
print(f"  Min:    {lengths.min()}")
print(f"  25th:   {int(lengths.quantile(0.25))}")
print(f"  Median: {int(lengths.median())}")
print(f"  75th:   {int(lengths.quantile(0.75))}")
print(f"  95th:   {int(lengths.quantile(0.95))}")
print(f"  Max:    {lengths.max()}")
print(f"  Mean:   {lengths.mean():.0f}")

# Approximate token count (rough: chars / 4)
approx_tokens = lengths / 4
print(f"\n--- Approximate Token Counts ---")
print(f"  Median: {int(approx_tokens.median())}")
print(f"  95th:   {int(approx_tokens.quantile(0.95))}")
print(f"  Max:    {int(approx_tokens.max())}")

# Save
print(f"\nWriting {OUTPUT_FILE}...")
df_out.to_parquet(OUTPUT_FILE, index=False)
print(f"Done. {len(df_out)} rows written.")
