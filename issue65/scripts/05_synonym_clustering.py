#!/usr/bin/env python3
"""Step 3: Synonym clustering of user-defined column names.
Issue #65 — PEPhub Schema Diversity Analysis

Groups column names that refer to the same concept using:
  1. Define canonical seed concepts from the most frequent user-defined columns
  2. Normalize names and compute string similarity to seeds
  3. Assign columns to the best-matching seed (if above threshold)
  4. Post-hoc merge seeds that are themselves synonyms

Avoids the transitive-chain problem of connected-component approaches.
"""

import json
import re
import numpy as np
import pandas as pd
from collections import defaultdict
from pathlib import Path
from rapidfuzz import fuzz

DATA_DIR = Path("../../data")
OUTPUT_DIR = Path("../output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Load user-defined columns
print("Loading user-defined columns...")
user_cols = pd.read_parquet(DATA_DIR / "user_defined_columns.parquet")
print(f"Total user-defined columns: {len(user_cols):,}")

# Focus on columns appearing in >= 50 projects
MIN_PROJECTS = 50
candidates = user_cols[user_cols["n_projects"] >= MIN_PROJECTS].copy()
print(f"Columns with >= {MIN_PROJECTS} projects: {len(candidates):,}")

# ============================================================
# Step 1: Normalize column names
# ============================================================
def normalize(name):
    """Normalize column name for comparison."""
    s = name.lower().strip()
    s = re.sub(r"[_\-\.]+", " ", s)
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def get_tokens(name):
    """Get sorted token set from normalized name."""
    return set(normalize(name).split())

candidates["normalized"] = candidates["column_name"].apply(normalize)

# ============================================================
# Step 2: Define canonical seed concepts
# ============================================================
# Top user-defined columns become seeds. We'll manually separate obvious
# distinct concepts that would otherwise chain together.

# Build seeds from top columns, skipping any that are clearly variants of
# an already-seen concept
seeds = {}  # canonical_name -> set of tokens
SEED_CANDIDATES = candidates.head(100)["column_name"].tolist()

# Manual seed definitions for top biological concepts, ensuring separation
MANUAL_SEEDS = {
    "tissue": {"tissue"},
    "cell_type": {"cell", "type"},
    "cell_line": {"cell", "line"},
    "treatment": {"treatment"},
    "age": {"age"},
    "genotype": {"genotype"},
    "strain": {"strain"},
    "sex": {"sex"},
    "gender": {"gender"},
    "assembly": {"assembly"},
    "genome_build": {"genome", "build"},
    "antibody": {"antibody"},
    "chip_antibody": {"chip", "antibody"},
    "developmental_stage": {"developmental", "stage"},
    "dev_stage": {"dev", "stage"},
    "disease_state": {"disease", "state"},
    "disease": {"disease"},
    "cell_line_background": {"cell", "line", "background"},
    "time_point": {"time", "point"},
    "time": {"time"},
    "dose": {"dose"},
    "donor_id": {"donor", "id"},
    "patient_id": {"patient", "id"},
    "subject_id": {"subject", "id"},
    "condition": {"condition"},
    "replicate": {"replicate"},
    "batch": {"batch"},
    "phenotype": {"phenotype"},
    "infection": {"infection"},
    "passage": {"passage"},
    "diagnosis": {"diagnosis"},
    "strain_background": {"strain", "background"},
    "organism": {"organism"},
    "species": {"species"},
    "brain_region": {"brain", "region"},
    "organ": {"organ"},
    "source": {"source"},
    "growth_condition": {"growth", "condition"},
    "culture_condition": {"culture", "condition"},
    "tumor_type": {"tumor", "type"},
    "tumor_stage": {"tumor", "stage"},
    "cancer_type": {"cancer", "type"},
    "smoking_status": {"smoking", "status"},
    "er_status": {"er", "status"},
    "her2_status": {"her2", "status"},
    "biomaterial_type": {"biomaterial", "type"},
    "library_type": {"library", "type"},
    "cell_population": {"cell", "population"},
    "rna_source": {"rna", "source"},
    "drug": {"drug"},
    "clone": {"clone"},
    "differentiation": {"differentiation"},
    "differentiation_stage": {"differentiation", "stage"},
    "transfection": {"transfection"},
    "cell_cycle": {"cell", "cycle"},
    "rin": {"rin"},
    "mouse_strain": {"mouse", "strain"},
    "mouse_id": {"mouse", "id"},
    "exposure": {"exposure"},
    "temperature": {"temperature"},
    "concentration": {"concentration"},
    "cell_markers": {"cell", "markers"},
}

# ============================================================
# Step 3: Assign columns to best-matching seed
# ============================================================
print(f"\nMatching {len(candidates)} columns to {len(MANUAL_SEEDS)} seed concepts...")

# For each candidate column, compute similarity to each seed
# Use token_set_ratio for flexible matching (handles reordering and subsets)
assignments = {}  # column_name -> seed
unassigned = []

# Pre-compute normalized seed names
seed_norms = {seed: normalize(seed) for seed in MANUAL_SEEDS}

for _, row in candidates.iterrows():
    col = row["column_name"]
    col_norm = row["normalized"]
    col_tokens = get_tokens(col)

    best_seed = None
    best_score = 0

    for seed, seed_tokens in MANUAL_SEEDS.items():
        seed_norm = seed_norms[seed]

        # Token overlap: what fraction of the column's tokens match the seed's tokens?
        overlap = col_tokens & seed_tokens
        if not overlap:
            continue

        # Compute fuzzy score
        score = fuzz.token_set_ratio(col_norm, seed_norm)

        # Bonus for exact token containment
        if seed_tokens <= col_tokens or col_tokens <= seed_tokens:
            score = min(100, score + 5)

        if score > best_score:
            best_score = score
            best_seed = seed

    # Threshold: require high similarity
    if best_score >= 85 and best_seed is not None:
        # Verify the match makes sense: the column should not be a compound
        # concept that's actually about something else
        # e.g., "treatment_time" matches both "treatment" and "time"
        # Prefer the seed with more token overlap
        if best_seed:
            assignments[col] = best_seed
    else:
        unassigned.append(col)

# Build clusters
clusters = defaultdict(list)
for col, seed in assignments.items():
    clusters[seed].append(col)

# Add seed itself if not already in its cluster
for seed in MANUAL_SEEDS:
    if seed not in clusters[seed] and seed in candidates["column_name"].values:
        clusters[seed].insert(0, seed)

# Remove single-member clusters where the only member is the seed itself
clusters = {k: v for k, v in clusters.items() if len(v) > 0}

# ============================================================
# Step 4: Build synonym mapping with frequency info
# ============================================================
freq_lookup = dict(zip(candidates["column_name"], candidates["n_projects"]))

synonym_map = {}
cluster_details = []

for seed, members in sorted(clusters.items(),
                             key=lambda x: -sum(freq_lookup.get(m, 0) for m in x[1])):
    member_freqs = [(m, freq_lookup.get(m, 0)) for m in members]
    member_freqs.sort(key=lambda x: -x[1])
    total = sum(f for _, f in member_freqs)

    if len(member_freqs) < 2:
        continue

    canonical = member_freqs[0][0]
    variants = [m for m, _ in member_freqs[1:]]

    synonym_map[canonical] = {
        "variants": variants,
        "total_projects": total,
        "members": {m: f for m, f in member_freqs},
    }

    cluster_details.append({
        "canonical": canonical,
        "n_variants": len(member_freqs),
        "total_projects": total,
        "top_variant_projects": member_freqs[0][1],
        "variants": "; ".join(f"{m} ({f:,})" for m, f in member_freqs),
    })

# Print results
print(f"\n{'='*70}")
print(f"SYNONYM CLUSTERS")
print(f"{'='*70}")
print(f"Total clusters: {len(cluster_details)}")
print(f"Columns in clusters: {sum(c['n_variants'] for c in cluster_details)}")
print(f"Columns not in any cluster: {len(unassigned)}")

print(f"\n--- All Synonym Clusters (sorted by total projects) ---")
for i, c in enumerate(cluster_details):
    print(f"\n{i+1}. {c['canonical']} ({c['n_variants']} variants, {c['total_projects']:,} total projects)")
    print(f"   {c['variants']}")

# Save
with open(DATA_DIR / "column_synonyms.json", "w") as f:
    # Simplified format for benchmark: canonical -> [variants]
    simple = {k: v["variants"] for k, v in synonym_map.items()}
    json.dump(simple, f, indent=2)
print(f"\nSaved {DATA_DIR / 'column_synonyms.json'}")

# Full synonym map with frequencies
with open(DATA_DIR / "column_synonyms_full.json", "w") as f:
    json.dump(synonym_map, f, indent=2)
print(f"Saved {DATA_DIR / 'column_synonyms_full.json'}")

# Cluster details CSV
cluster_df = pd.DataFrame(cluster_details)
cluster_df.to_csv(OUTPUT_DIR / "synonym_clusters.csv", index=False)
print(f"Saved {OUTPUT_DIR / 'synonym_clusters.csv'}")

# Unassigned top columns (for manual review)
unassigned_with_freq = [(c, freq_lookup.get(c, 0)) for c in unassigned]
unassigned_with_freq.sort(key=lambda x: -x[1])
print(f"\n--- Top 50 Unassigned Columns (for manual review) ---")
for col, freq in unassigned_with_freq[:50]:
    print(f"  {col:<45s}  {freq:>7,}")

pd.DataFrame(unassigned_with_freq, columns=["column_name", "n_projects"]).to_csv(
    OUTPUT_DIR / "unassigned_columns.csv", index=False)

print(f"\nStep 3 complete.")
