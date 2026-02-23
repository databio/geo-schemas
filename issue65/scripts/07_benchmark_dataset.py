#!/usr/bin/env python3
"""Step 5: Curate benchmark dataset for schema mapping evaluation.
Issue #65 — PEPhub Schema Diversity Analysis

Produces a clean ground-truth mapping file for MetaHarmony Sub-Aim 1.3:
  - Merges overlapping synonym clusters (e.g., sex/gender)
  - Removes internal-only/infrastructure column names
  - Outputs: {"canonical_name": ["variant1", "variant2", ...]}
"""

import json
import pandas as pd
from pathlib import Path

DATA_DIR = Path("../../data")
OUTPUT_DIR = Path("../output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Load synonym data
with open(DATA_DIR / "column_synonyms_full.json") as f:
    synonyms_full = json.load(f)

# Load frequency data for filtering
user_cols = pd.read_parquet(DATA_DIR / "user_defined_columns.parquet")
freq_lookup = dict(zip(user_cols["column_name"], user_cols["n_projects"]))

# ============================================================
# Step 1: Merge overlapping clusters
# ============================================================
# Some concepts have overlapping seeds. Merge them manually.
MERGE_MAP = {
    # sex and gender are the same concept
    "sex": ["sex", "gender"],
    # developmental stage variants
    "developmental_stage": ["developmental_stage", "dev_stage"],
    # disease-related
    "disease": ["disease", "disease_state"],
    # time-related
    "time_point": ["time"],
    # antibody (includes chip_antibody as a specialization)
    "antibody": ["antibody", "chip_antibody"],
    # growth and culture conditions
    "growth_condition": ["growth_condition", "culture_condition"],
    # replicate
    "replicate": ["biological_replicate_number"],
    # donor/patient/subject identifiers
    "subject_id": ["donor_id", "patient_id", "subject_id"],
}

# Build merged clusters
merged = {}
consumed = set()

for target, sources in MERGE_MAP.items():
    all_members = {}
    for src in sources:
        if src in synonyms_full:
            for member, count in synonyms_full[src]["members"].items():
                if member not in all_members or count > all_members[member]:
                    all_members[member] = count
            consumed.add(src)
    if all_members:
        merged[target] = all_members

# Add remaining non-consumed clusters
for canonical, data in synonyms_full.items():
    if canonical not in consumed:
        merged[canonical] = data["members"]

# ============================================================
# Step 2: Clean up each cluster
# ============================================================
# Remove members that are:
# - microarray signal columns (ch1_*, ch2_*, f635_*, etc.)
# - ENCODE-specific infrastructure
# - very low frequency (< 50 projects)

EXCLUDE_PREFIXES = ["ch1_", "ch2_", "f635_", "f532_", "b635_", "b532_",
                    "cy5_", "cy3_", "rat", "rgn_", "tot_"]
EXCLUDE_EXACT = {"id", "type", "state", "r", "a", "line", "value",
                 "growth", "culture", "name", "cell", "source",
                 "detection", "raw", "input", "background", "chip",
                 "run", "slide", "block", "row", "flag", "call",
                 "rna", "mouse", "er", "status", "medium", "media"}

def should_include(col_name, count):
    """Check if a column name belongs in the benchmark."""
    if count < 50:
        return False
    if col_name in EXCLUDE_EXACT:
        return False
    for prefix in EXCLUDE_PREFIXES:
        if col_name.startswith(prefix):
            return False
    return True

benchmark = {}
for canonical, members in sorted(merged.items(), key=lambda x: -sum(x[1].values())):
    clean_members = {k: v for k, v in members.items() if should_include(k, v)}
    if len(clean_members) < 2:
        continue

    # Pick canonical as the most frequent member
    sorted_members = sorted(clean_members.items(), key=lambda x: -x[1])
    best_canonical = sorted_members[0][0]
    variants = [m for m, _ in sorted_members[1:]]

    benchmark[best_canonical] = variants

# ============================================================
# Step 3: Output
# ============================================================
# Simple format: canonical -> [variants]
with open(DATA_DIR / "schema_mapping_benchmark.json", "w") as f:
    json.dump(benchmark, f, indent=2)

# Also output with frequency annotations
benchmark_annotated = {}
for canonical, variants in benchmark.items():
    all_names = [canonical] + variants
    benchmark_annotated[canonical] = {
        "variants": variants,
        "n_variants": len(variants) + 1,
        "frequencies": {n: freq_lookup.get(n, 0) for n in all_names},
        "total_projects": sum(freq_lookup.get(n, 0) for n in all_names),
    }

with open(DATA_DIR / "schema_mapping_benchmark_annotated.json", "w") as f:
    json.dump(benchmark_annotated, f, indent=2)

# Summary CSV
rows = []
for canonical, data in sorted(benchmark_annotated.items(),
                                key=lambda x: -x[1]["total_projects"]):
    rows.append({
        "canonical": canonical,
        "n_variants": data["n_variants"],
        "total_projects": data["total_projects"],
        "variants": "; ".join(data["variants"]),
    })

summary_df = pd.DataFrame(rows)
summary_df.to_csv(OUTPUT_DIR / "benchmark_summary.csv", index=False)

# Print results
print("=" * 70)
print("SCHEMA MAPPING BENCHMARK DATASET")
print("=" * 70)
print(f"Canonical concepts: {len(benchmark)}")
print(f"Total variant names: {sum(len(v) for v in benchmark.values())}")
print(f"Total unique names: {sum(len(v) + 1 for v in benchmark.values())}")

print(f"\n--- All Concepts ---")
for canonical, data in sorted(benchmark_annotated.items(),
                                key=lambda x: -x[1]["total_projects"]):
    freqs = data["frequencies"]
    canon_freq = freqs.get(canonical, 0)
    print(f"\n  {canonical} ({canon_freq:,} projects, {data['n_variants']} total names)")
    for v in data["variants"]:
        print(f"    -> {v} ({freqs.get(v, 0):,})")

print(f"\nBenchmark saved to:")
print(f"  {DATA_DIR / 'schema_mapping_benchmark.json'}")
print(f"  {DATA_DIR / 'schema_mapping_benchmark_annotated.json'}")
print(f"  {OUTPUT_DIR / 'benchmark_summary.csv'}")
print(f"\nStep 5 complete.")
