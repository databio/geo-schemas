# geo-schemas

Exploratory analyses of GEO (Gene Expression Omnibus) metadata from the [PEPhub](https://pephub.databio.org/) `geo` namespace (~229K projects). Work is organized around two discussions on [databio/lab](https://github.com/databio/lab.databio.org):

## Issues

### Issue 55 — GEO Series-Level Semantic Meta-Analysis

Embeds series-level text (title + summary + design) with sentence-transformers, projects into 2D with UMAP, and clusters with HDBSCAN/k-means to map the landscape of GEO research topics. Includes temporal trend analysis, country/institution profiles, and centroid trajectory tracking across eras.

### Issue 65 — PEPhub Schema Diversity Analysis

Analyzes the sample-table column names across all GEO PEPs to characterize schema diversity. Classifies columns as GEO-standard vs. user-defined, clusters synonymous column names (e.g. `tissue`, `tissue_type`, `Tissue`), and produces a benchmark dataset for schema mapping evaluation.

## Repo structure

```
preprocessing/          Shared data ingestion (download archive, parse PEPs)
issue55/
  scripts/              Analysis scripts (numbered, run in order from 03)
  output/               Figures, CSVs, cluster results, report
issue65/
  scripts/              Analysis scripts (numbered, run in order from 03)
  output/               Figures, CSVs, benchmark files, report
data/                   Shared data dir (gitignored — large parquet files)
plans/                  Session plans and implementation logs
```

Scripts are numbered to indicate execution order. Steps 00-02 in `preprocessing/` are shared (data download and parsing); steps 03+ are issue-specific.

## Setup

1. R packages: `arrow`, `data.table`, `ggplot2`, `lubridate`, `fs`, `yaml`, `httr2`, `furrr`, `progressr`, `here`
2. Python packages: `pandas`, `pyarrow`, `numpy`, `sentence-transformers`, `umap-learn`, `hdbscan`, `scikit-learn`, `matplotlib`, `rapidfuzz`, `adjustText`, `python-dotenv`
3. Run `preprocessing/01_download_archive.R` to fetch the GEO archive, then `preprocessing/02_parse_peps.R` to build `data/geo_metadata.parquet`.
4. Run issue-specific scripts from their `scripts/` directory (they use relative paths expecting `cwd` = `issueNN/scripts/`).
