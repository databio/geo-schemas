# Issue 65 — PEPhub Schema Diversity Analysis

Analyzes sample-table column names across ~229K GEO PEPs to characterize schema diversity, identify synonym clusters, and produce a benchmark dataset for schema mapping evaluation.

## Scripts

Run from `scripts/` directory, in order:

| Script | What it does |
|--------|-------------|
| `03_column_frequency.py` | Explodes column names, counts frequency of each unique name |
| `04_standard_vs_user.py` | Classifies columns as GEO-standard vs user-defined |
| `05_synonym_clustering.py` | Groups variant column names into synonym clusters |
| `06_publication_figures.py` | Generates figures (long-tail, synonyms, complexity, harmonization gap) |
| `07_benchmark_dataset.py` | Curates a ground-truth benchmark for schema mapping evaluation |

## Key output files

### Unique column name records

- **`output/column_frequencies.csv`** — Complete list of every unique column name with project count and percentage. This is the full inventory.
- **`output/column_classifications.csv`** — Same list with an `is_standard` boolean flag separating GEO-injected infrastructure columns from user-defined ones.
- **`output/user_defined_columns.csv`** — Filtered to just user-defined columns (GEO infrastructure stripped out). This is the curated subset most useful for analysis.

### Synonym clustering

- **`output/synonym_clusters.csv`** — Synonym groups with canonical name, variant count, and member list.
- **`output/unassigned_columns.csv`** — Columns not matched to any synonym cluster (candidates for manual review).

### Benchmark

- **`output/benchmark_summary.csv`** — Ground-truth mapping of canonical column names to their variants, for evaluating schema harmonization tools.

### Figures

- `output/longtail_distribution.png` — Column usage frequency (log-log rank plot)
- `output/synonym_clusters_top20.png` — Top 20 biological concepts and variant counts
- `output/schema_complexity_histogram.png` — Columns-per-PEP distribution
- `output/harmonization_gap.png` — Canonical name vs variant usage for top concepts
