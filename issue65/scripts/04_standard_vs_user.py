#!/usr/bin/env python3
"""Step 2: Classify GEO-standard vs user-defined columns.
Issue #65 — PEPhub Schema Diversity Analysis

GEO-standard columns are injected by GEOfetch and appear in nearly every PEP.
User-defined columns are what submitters added and represent the real schema
diversity we want to analyze.

Classification approach:
  1. Known GEO/SRA/GEOfetch prefixes (sample_*, srr, srx, etc.)
  2. Frequency-based backup: columns in >90% of projects are likely standard
"""

import json
import pandas as pd
from pathlib import Path

DATA_DIR = Path("../../data")
OUTPUT_DIR = Path("../output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

FREQ_FILE = DATA_DIR / "column_frequencies.parquet"
OUTPUT_FILE = DATA_DIR / "column_classifications.parquet"

# Known GEO/SRA/GEOfetch standard column patterns
# These are injected by the GEO SOFT format or GEOfetch pipeline, not by submitters
STANDARD_PREFIXES = [
    "sample_geo_accession",
    "sample_status",
    "sample_submission_date",
    "sample_last_update_date",
    "sample_type",
    "sample_channel_count",
    "sample_molecule_ch",
    "sample_platform_id",
    "sample_taxid_ch",
    "sample_organism_ch",
    "sample_source_name_ch",
    "sample_label_ch",
    "sample_label_protocol_ch",
    "sample_extract_protocol_ch",
    "sample_hyb_protocol",
    "sample_scan_protocol",
    "sample_data_processing",
    "sample_data_row_count",
    "sample_contact_",        # all contact fields
    "sample_supplementary_file",
    "sample_series_id",
    "sample_relation",
    "sample_library_",        # library_selection, _source, _strategy
    "sample_instrument_model",
    "sample_description",
    "sample_title",
    "sample_name",
    "sample_characteristics_ch",
    "sample_growth_protocol_ch",
    "sample_treatment_protocol_ch",
]

STANDARD_EXACT = {
    # GEOfetch-injected columns
    "sample_name", "protocol", "organism", "read_type", "data_source",
    "srr", "srx", "big_key", "gsm_id", "sra", "biosample",
    # Common GEO soft fields
    "id_ref", "value",
    "pep_version",
}

def is_standard(col_name):
    """Check if a column name is a GEO/GEOfetch standard field."""
    if col_name in STANDARD_EXACT:
        return True
    for prefix in STANDARD_PREFIXES:
        if col_name.startswith(prefix):
            return True
    return False

# Load frequencies
print("Loading column frequencies...")
freq = pd.read_parquet(FREQ_FILE)
print(f"Total unique columns: {len(freq):,}")

# Classify
freq["is_standard"] = freq["column_name"].apply(is_standard)

# Also flag very-high-frequency columns that aren't caught by name patterns
# (safety net: if it appears in >90% of projects, it's likely infrastructure)
high_freq_mask = (freq["pct_projects"] >= 90) & (~freq["is_standard"])
if high_freq_mask.any():
    print(f"\nHigh-frequency columns not in standard list:")
    for _, row in freq[high_freq_mask].iterrows():
        print(f"  {row['column_name']:<45s}  {row['pct_projects']:.2f}%")
    # Mark them as standard too
    freq.loc[high_freq_mask, "is_standard"] = True

n_standard = freq["is_standard"].sum()
n_user = (~freq["is_standard"]).sum()

print(f"\n--- Classification Results ---")
print(f"Standard columns:    {n_standard:>6,}")
print(f"User-defined columns: {n_user:>6,}")

# Save
freq.to_parquet(OUTPUT_FILE, index=False)
freq.to_csv(OUTPUT_DIR / "column_classifications.csv", index=False)
print(f"Saved {OUTPUT_FILE}")

# Show top standard columns
print(f"\n--- Top 30 Standard Columns ---")
std = freq[freq["is_standard"]].head(30)
for _, row in std.iterrows():
    print(f"  {row['column_name']:<45s}  {row['n_projects']:>7,}  ({row['pct_projects']:>6.2f}%)")

# Show top user-defined columns
print(f"\n--- Top 50 User-Defined Columns ---")
usr = freq[~freq["is_standard"]].head(50)
for _, row in usr.iterrows():
    print(f"  {row['column_name']:<45s}  {row['n_projects']:>7,}  ({row['pct_projects']:>6.2f}%)")

# Save separate user-defined list for downstream analysis
user_cols = freq[~freq["is_standard"]].copy()
user_cols.to_parquet(DATA_DIR / "user_defined_columns.parquet", index=False)
user_cols.to_csv(OUTPUT_DIR / "user_defined_columns.csv", index=False)
print(f"\nUser-defined columns saved: {len(user_cols):,}")

print(f"\nStep 2 complete. Output in: {OUTPUT_DIR}")
