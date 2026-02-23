---
date: 2026-02-23
status: complete
description: Reorganize geo-schemas repo by issue into self-contained workspaces
---

# Reorganize geo-schemas by issue

## Context

This repo contains work for two GitHub discussions (#55 and #65) on databio/lab.databio.org. The output is already split into `output/issue55/` and `output/issue65/`, but scripts are interleaved in flat `python/` and `R/` dirs with `a`/`b` suffixes. One script (`09b_fix_era_plots.py`) is mislabeled — it's actually issue 55 work. The goal is to make this pushable with clear per-issue organization.

## Target structure

```
geo-schemas/
  shared/                          # Data ingestion (both issues depend on this)
    00_test_setup.R
    01_download_archive.R
    02_parse_peps.R
    02_parse_peps_api.R
    test_parquet.R

  issue55/                         # GEO series-level semantic meta-analysis
    scripts/
      03_categorical_analysis.R
      04_text_preprocessing.py
      05_generate_embeddings.py
      06_umap_clustering.py
      07_cluster_labeling.py
      08_temporal_institutional.py
      09_recency_analysis.py
      09_fix_era_plots.py          # was 09b_ (mislabeled)
      10_centroid_analysis.py
      report_stats.py
    output/                        # moved from output/issue55/
      report/
      ...

  issue65/                         # PEPhub schema diversity analysis
    scripts/
      03_column_frequency.py
      04_standard_vs_user.py
      05_synonym_clustering.py
      06_publication_figures.py
      07_benchmark_dataset.py
    output/                        # moved from output/issue65/
      reports/
      ...

  data/                            # Shared root data (unchanged location)
    geo_metadata.parquet
    geo_2024_10_01.tar
    peps/
    geo_text_for_embedding.parquet  # issue55 intermediate
    geo_embeddings.parquet          # issue55 intermediate
    column_frequencies.parquet      # issue65 intermediate
    column_classifications.parquet  # issue65 intermediate
    user_defined_columns.parquet    # issue65 intermediate
    column_synonyms.json            # issue65 intermediate
    column_synonyms_full.json       # issue65 intermediate
    schema_mapping_benchmark.json   # issue65 intermediate
    schema_mapping_benchmark_annotated.json

  plans/                           # Stays at root
  .venv/                           # Stays at root
  .env, .gitignore, geo-schemas.Rproj  # Stay at root
```

**Note:** Intermediate data files stay in shared `data/` rather than splitting into per-issue data dirs. The shared parquet files are large (1GB+) and both workstreams read `geo_metadata.parquet`. Keeping `data/` unified avoids duplication and keeps the reorg simple. The `data/` dir is gitignored anyway.

## Steps

### 1. Create new directory structure
```
mkdir -p shared issue55/scripts issue65/scripts
```

### 2. Move shared R scripts → `shared/`
```
R/00_test_setup.R      → shared/00_test_setup.R
R/01_download_archive.R → shared/01_download_archive.R
R/02_parse_peps.R      → shared/02_parse_peps.R
R/02_parse_peps_api.R  → shared/02_parse_peps_api.R
R/test_parquet.R       → shared/test_parquet.R
```

### 3. Move issue 55 scripts → `issue55/scripts/` (drop `a` suffix)
```
R/03a_categorical_analysis.R       → issue55/scripts/03_categorical_analysis.R
python/04a_text_preprocessing.py   → issue55/scripts/04_text_preprocessing.py
python/05a_generate_embeddings.py  → issue55/scripts/05_generate_embeddings.py
python/06a_umap_clustering.py      → issue55/scripts/06_umap_clustering.py
python/07a_cluster_labeling.py     → issue55/scripts/07_cluster_labeling.py
python/08a_temporal_institutional.py → issue55/scripts/08_temporal_institutional.py
python/09a_recency_analysis.py     → issue55/scripts/09_recency_analysis.py
python/09b_fix_era_plots.py        → issue55/scripts/09_fix_era_plots.py  (fix mislabel)
python/10a_centroid_analysis.py    → issue55/scripts/10_centroid_analysis.py
python/report_stats.py             → issue55/scripts/report_stats.py
```

### 4. Move issue 65 scripts → `issue65/scripts/` (drop `b` suffix)
```
python/03b_column_frequency.py     → issue65/scripts/03_column_frequency.py
python/04b_standard_vs_user.py     → issue65/scripts/04_standard_vs_user.py
python/05b_synonym_clustering.py   → issue65/scripts/05_synonym_clustering.py
python/06b_publication_figures.py  → issue65/scripts/06_publication_figures.py
python/07b_benchmark_dataset.py    → issue65/scripts/07_benchmark_dataset.py
```

### 5. Move output directories
```
output/issue55/* → issue55/output/
output/issue65/* → issue65/output/
```

### 6. Remove empty old directories
```
rmdir R/ python/ output/
```

### 7. Update file paths inside scripts

All scripts currently use paths relative to the project root. After moving to `issueNN/scripts/`, paths need to go up two levels to reach root-level dirs.

**Pattern for all scripts:**
- `data/X` → `../../data/X`
- `output/issue55/X` → `../output/X` (in issue55 scripts)
- `output/issue65/X` → `../output/X` (in issue65 scripts)

**Pattern for shared R scripts:**
- `data/X` → `../data/X`

**Files to update (17 total):**
- `shared/`: 5 R scripts (update `data/` refs)
- `issue55/scripts/`: 10 scripts (update `data/` and `output/issue55/` refs)
- `issue65/scripts/`: 5 scripts (update `data/` and `output/issue65/` refs)

### 8. Handle `02_parse_peps.R` special case
This script references `data/geo_archive.tar` and `data/geo/` — update to `../data/` prefix.

## Verification

1. `grep -r "output/issue55" issue55/` — should return 0 matches (old paths gone)
2. `grep -r "output/issue65" issue65/` — should return 0 matches
3. `grep -rn "\"data/" issue55/ issue65/ shared/` — should show only `../../data/` or `../data/` patterns
4. No leftover files in old `R/`, `python/`, `output/` directories
