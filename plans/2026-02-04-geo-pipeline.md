# GEO Metadata Pipeline Plan

## Overview

Shared data pipeline for two projects:
- **#65**: PEPhub Schema Diversity Analysis (column names)
- **#55**: GEO Series-Level Semantic Meta Analysis (embeddings)

## Data Source

- **Archive**: `https://cloud2.databio.org/pephub/geo/geo_2025_04_28.tar`
- **Size**: ~1.4 GB, 239,685 projects
- **Current API count**: 254,902 projects (archive slightly behind)

## Pipeline Scripts

```
R/
├── 01_download_archive.R   # Download and extract tar archive
├── 02_parse_peps.R         # Parse extracted files (fast, requires download)
└── 02_parse_peps_api.R     # Alternative: fetch via API (slow, no download)
```

### Usage

```r
# Option A: Archive (recommended for full dataset)
source("R/01_download_archive.R")
download_geo_archive()
extract_geo_archive()

source("R/02_parse_peps.R")
dt <- parse_all_peps(n_workers = 8)
save_metadata(dt)

# Option B: API (for testing without 1.4GB download)
source("R/02_parse_peps_api.R")
dt <- fetch_all_projects(n_projects = 100)
```

## Output Schema

`data/geo_metadata.parquet`:

| Column | Type | Use |
|--------|------|-----|
| `gse_id` | string | GSE accession |
| `n_samples` | int | Sample count |
| `n_columns` | int | Number of columns in sample table |
| `column_names_json` | string (JSON array) | Column names for #65 |
| `series_title` | string | Study title for #55 |
| `series_summary` | string | Study description for #55 |
| `series_type` | string | Assay type |
| `series_organism` | string | Species |
| `series_contributor` | string | Authors |
| `series_contact_country` | string | Country for regional analysis |
| `series_contact_institute` | string | Institution |
| `series_submission_date` | string | For temporal analysis |
| `series_platform_id` | string | GPL platform |

## R Packages Needed

```r
install.packages(c("fs", "yaml", "data.table", "arrow", "httr2", "furrr", "progressr", "here", "jsonlite"))
```

## Downstream Analysis (branch points)

- **#65**: `column_names_json` → frequency analysis → synonym clustering
- **#55**: `series_title` + `series_summary` → embeddings → UMAP
