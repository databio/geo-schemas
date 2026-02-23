# Issue 55 — GEO Series-Level Semantic Meta-Analysis

Embeds series-level metadata text (title + summary + overall design) for ~229K GEO projects, projects into 2D with UMAP, clusters with HDBSCAN and k-means, and analyzes the resulting research topic landscape across time, geography, and institution.

## Scripts

Run from `scripts/` directory, in order:

| Script | What it does |
|--------|-------------|
| `03_categorical_analysis.R` | Preliminary counts: series types, organisms, countries, institutions, year distribution |
| `04_text_preprocessing.py` | Concatenates and cleans text fields into `geo_text_for_embedding.parquet` |
| `05_generate_embeddings.py` | Generates 768-dim sentence-transformer embeddings (all-mpnet-base-v2) |
| `06_umap_clustering.py` | UMAP 2D projection + HDBSCAN and k-means (k=20,50,100) clustering |
| `07_cluster_labeling.py` | TF-IDF keyword extraction per cluster to generate interpretable labels |
| `08_temporal_institutional.py` | Pseudotime UMAP, cluster trends, country/institution/modality/organism profiles |
| `09_recency_analysis.py` | Era-stratified analysis: modality shifts, country changes, topic growth/decline |
| `09_fix_era_plots.py` | Regenerated era plots with corrected colors + institution-by-era UMAP grid |
| `10_centroid_analysis.py` | Centroid trajectory tracking for countries and topics across eras |
| `report_stats.py` | Extracts summary statistics as JSON for the report |

## Key output files

### Clustering results

- **`output/umap_clusters.parquet`** — UMAP coordinates + cluster assignments (HDBSCAN, k-means at k=20/50/100) for every project
- **`output/cluster_labels_k50.csv`** — Interpretable labels for each k=50 cluster (top TF-IDF terms, organisms, types, countries)
- **`output/cluster_labels_hdbscan.csv`** — Same for HDBSCAN clusters
- **`output/cluster_stats.csv`** — Per-cluster size, mean year, UMAP spread

### Categorical summaries

- `output/series_type_counts.csv`, `organism_counts.csv`, `country_counts.csv`, `institution_counts.csv`, `contributor_top500.csv`, `projects_per_year.csv`

### Temporal analysis

- **`output/recency_stats.json`** — Era counts, modality/country shifts, top growing and declining topics
- **`output/centroid_stats.json`** — Country and topic centroid displacements across eras

### Figures

All in `output/` and `output/report/`. Highlights:

- `umap_clusters.png` — HDBSCAN and k-means side by side
- `umap_by_era.png` — Four-panel UMAP colored by submission era
- `umap_by_modality.png`, `umap_by_organism.png` — Six/five-panel breakdowns
- `umap_countries.png`, `umap_institutions.png` — Geographic/institutional profiles
- `topic_growth_decline.png` — Fastest growing and declining research topics
- `centroid_countries.png`, `centroid_topics.png` — Trajectory arrows across eras

### Report

- `output/report/report.md` — Full writeup with embedded figures
