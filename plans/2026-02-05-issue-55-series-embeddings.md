# Issue #55: GEO Series-Level Semantic Meta Analysis

## Context

PEPhub focuses on sample-level (GSM) metadata, but there is rich series-level (GSE) metadata that has not been systematically analyzed. This metadata includes study titles, descriptions, authors/contributors, institutions, countries, submission dates, organism, and assay types. By generating text embeddings for series metadata, we can see how GEO studies cluster semantically and explore how research interests vary by institution, region, and time.

This project shares data infrastructure with #65 (both use the same GEO archive and parsed parquet), but diverges into embedding-based analysis rather than schema mapping.

From the parsed archive, all 229,101 projects have populated `series_title` and `series_summary` fields, plus structured metadata on contributors, institutions, countries, and submission dates.

**Source**: https://github.com/databio/lab.databio.org/discussions/55

## Starting Point

### Project layout

```
geo-schemas/
├── R/
│   ├── 00_test_setup.R          # Verifies packages + API connectivity
│   ├── 01_download_archive.R    # Downloads GEO namespace tar from PEPhub
│   ├── 02_parse_peps.R          # Parses tar→zip→yaml/csv into parquet
│   └── 02_parse_peps_api.R      # Alternative: fetch via PEPhub REST API
├── data/
│   ├── geo_2024_10_01.tar       # GEO namespace archive (1.3 GB, already downloaded)
│   ├── peps/                    # Extracted zip files (229,101 zips in batch dirs)
│   └── geo_metadata.parquet     # Parsed output (163 MB, 229,101 rows)
├── plans/
└── output/
```

### Parquet schema: `data/geo_metadata.parquet`

229,101 rows, 16 columns:

| Column | Type | Description |
|--------|------|-------------|
| `gse_id` | string | GSE accession (e.g. "gse100000") |
| `n_samples` | int32 | Number of samples in the project |
| `n_columns` | int32 | Number of columns in the sample table |
| `series_title` | string | Study title |
| `series_summary` | string | Study description/abstract |
| `series_type` | string | Assay type (e.g. "Expression profiling by high throughput sequencing") |
| `series_organism` | string | Species |
| `series_contributor` | string | Authors ("+"-delimited, e.g. "John,,Doe + Jane,,Smith") |
| `series_contact_country` | string | Country of submitting institution |
| `series_contact_institute` | string | Submitting institution name |
| `series_submission_date` | string | Submission date (e.g. "Jun 13 2017") |
| `series_platform_id` | string | GPL platform ID |
| `series_overall_design` | string | Experimental design description |
| `description` | string | PEPhub project description (has GEO link + title) |
| `pep_version` | string | PEP spec version |
| `column_names_json` | string | JSON array of sample table column names (used by #65, not this analysis) |

### Key fields for this analysis

- **Text fields** for embeddings: `series_title`, `series_summary`, `series_overall_design`
- **Categorical fields** for stratification: `series_type`, `series_organism`, `series_contact_country`, `series_contact_institute`
- **Temporal field**: `series_submission_date` (string, needs parsing — format is "Mon DD YYYY", e.g. "Jun 13 2017")
- **Contributors**: `series_contributor` ("+"-delimited names in "Last,,First" format)

### R packages needed

```r
# R-side analysis and visualization
install.packages(c("arrow", "data.table", "ggplot2", "lubridate"))
```

Embedding generation will likely require Python (`sentence-transformers`). The R analysis can read the resulting embedding matrix from parquet.

## Current Status

- [x] Parsed series metadata into `data/geo_metadata.parquet`
- [x] Preliminary categorical analysis
- [x] Text preprocessing
- [x] Text embedding generation
- [x] UMAP/clustering
- [x] Cluster labeling
- [x] Temporal and institutional analysis

## Plan: Next Steps

### Step 1: Preliminary Categorical Analysis
- Distribution of `series_type` (expression profiling, ChIP-seq, ATAC-seq, etc.)
- Projects per year from `series_submission_date`
- Top organisms from `series_organism`
- Top countries from `series_contact_country`
- Top institutions from `series_contact_institute`
- Most prolific contributors from `series_contributor`

**Output**: Summary tables and basic plots characterizing the GEO landscape

### Step 2: Text Preprocessing
- Concatenate `series_title` + `series_summary` + `series_overall_design` into a combined text field
- Basic cleaning: collapse whitespace, handle encoding issues
- Evaluate text length distribution to inform embedding model choice (some models have token limits)

**Output**: `data/geo_text_for_embedding.parquet` with `gse_id` and `text` columns

### Step 3: Generate Embeddings
- Candidate models:
  - `sentence-transformers/all-MiniLM-L6-v2` (fast, 384-dim)
  - `sentence-transformers/all-mpnet-base-v2` (better quality, 768-dim)
  - `dmis-lab/biobert-v1.1` (biomedical domain)
- Run in Python (sentence-transformers) or via API, save embeddings as a matrix
- Consider batching (229K texts will need chunked processing)

**Output**: `data/geo_embeddings.parquet` with `gse_id` and embedding vector columns

### Step 4: UMAP and Clustering
- Run UMAP on embedding vectors for 2D visualization
- Try different clustering approaches:
  - k-means with varying k
  - HDBSCAN for density-based clusters
- Evaluate cluster coherence

**Output**: UMAP coordinates and cluster assignments per project

### Step 5: Cluster Labeling
- Two approaches from the discussion:
  1. **Keyword extraction**: frequent terms per cluster become labels (disease, tissue, method)
  2. **NER + ontology mapping**: extract biomedical entities, map to ontologies (MeSH, DO, UBERON)
- Could also use LLM-based summarization of cluster centroids

**Output**: Labeled clusters with interpretable names

### Step 6: Advanced Analyses
- **Pseudotime plots**: use `series_submission_date` to show how research interests shift over time
- **Institutional profiles**: compute embedding centroids per institution to characterize research focus
- **Regional analysis**: compare embedding distributions by `series_contact_country`
- **Cluster sparsity**: identify under-researched topics (sparse regions in embedding space)
- **Modality trends**: filter by `series_type` to see how applications of different assays (ATAC-seq, scRNA-seq) evolve over time

## Relationship to #65

These projects share the data pipeline (`02_parse_peps.R` and `geo_metadata.parquet`) but diverge after that:

- **#65** analyzes sample-table column names (schema diversity for grant data)
- **#55** analyzes series-level text metadata (research landscape via embeddings)

#65 is higher priority for near-term grant needs. #55 is more exploratory and could become a standalone paper on GEO research trends.

---

## Implementation Log (2026-02-05)

### Environment Setup

- **R 4.5.2**: `arrow`, `data.table`, `ggplot2`, `lubridate` (pre-installed)
- **Python 3.13.2**: venv at `.venv/`, managed with `uv`
- **Python packages**: `sentence-transformers`, `pyarrow`, `pandas`, `umap-learn`, `hdbscan`, `matplotlib`, `python-dotenv`
- **HF token**: stored in `.env` (read-only, for model downloads)

### Project Layout (post-implementation)

```
geo-schemas/
├── R/
│   ├── 03a_categorical_analysis.R       # Step 1
├── python/
│   ├── 04a_text_preprocessing.py        # Step 2
│   ├── 05a_generate_embeddings.py       # Step 3
│   ├── 06a_umap_clustering.py           # Step 4
│   ├── 07a_cluster_labeling.py          # Step 5
│   └── 08a_temporal_institutional.py    # Step 6
├── data/
│   ├── geo_metadata.parquet             # Input (163 MB, 229,101 rows)
│   ├── geo_text_for_embedding.parquet   # Step 2 output (combined text)
│   └── geo_embeddings.parquet           # Step 3 output (229K x 768)
├── output/issue55/
│   ├── series_type_counts.csv           # Step 1
│   ├── series_type_top15.png
│   ├── projects_per_year.csv
│   ├── projects_per_year.png
│   ├── organism_counts.csv
│   ├── organism_top15.png
│   ├── country_counts.csv
│   ├── country_top20.png
│   ├── institution_counts.csv
│   ├── institution_top20.png
│   ├── contributor_top500.csv
│   ├── umap_clusters.parquet            # Step 4 (UMAP coords + cluster labels)
│   ├── umap_clusters.png
│   ├── cluster_labels_k50.csv           # Step 5
│   ├── cluster_labels_hdbscan.csv
│   ├── cluster_stats.csv                # Step 6
│   ├── umap_pseudotime.png
│   ├── cluster_trends_top10.png
│   ├── umap_countries.png
│   ├── umap_institutions.png
│   └── modality_trends.png
├── .venv/                               # Python virtual environment
└── .env                                 # HF_TOKEN
```

### Step-by-Step Results

#### Step 1: Categorical Analysis (`R/03a_categorical_analysis.R`)

- 229,101 total projects spanning 2001-2024
- Top series types: RNA-seq (82.8K), microarray (63.6K), ChIP-seq (28.6K)
- Top organisms: Homo sapiens (96K), Mus musculus (70K), Drosophila (5.9K)
- Top countries: USA (107K), China (27K), Germany (13K), Japan (11K), UK (9K)
- Top institution: ENCODE DCC (16.2K) — dominates due to consortium submissions
- 845 unique series types, 5,562 unique organisms, 107 countries, 21,666 institutions

#### Step 2: Text Preprocessing (`python/04a_text_preprocessing.py`)

- Concatenated `series_title` + `series_summary` + `series_overall_design`
- All 229,101 rows had non-empty text
- Text length: median 942 chars (~235 tokens), 95th percentile 2,437 chars (~609 tokens)
- Output: `data/geo_text_for_embedding.parquet`

#### Step 3: Embeddings (`python/05a_generate_embeddings.py`)

- Model: `sentence-transformers/all-mpnet-base-v2` (768-dim, 384-token max)
- Device: MPS (Apple Silicon GPU) — 7x faster than CPU on real texts
- Benchmark: MPS 49 texts/sec vs CPU 7 texts/sec on real-length GEO texts
- Total time: 2h13m for 229K texts (29 texts/sec average, with thermal throttling)
- Batch size: 512
- Output: `data/geo_embeddings.parquet` (229,101 x 769 columns: gse_id + 768 embedding dims)

#### Step 4: UMAP & Clustering (`python/06a_umap_clustering.py`)

- UMAP: n_neighbors=30, min_dist=0.1, metric=cosine, 200 epochs — completed in 4 min
- HDBSCAN: 348 clusters, 73,392 noise points (32%), min_cluster_size=100
- K-means k=20: clusters range 6,241–18,974 (median 11,308)
- K-means k=50: clusters range 1,197–10,572 (median 4,230)
- K-means k=100: clusters range 339–5,551 (median 2,269)

#### Step 5: Cluster Labeling (`python/07a_cluster_labeling.py`)

- TF-IDF keyword extraction (10K features, 1-2 ngrams, English stop words)
- k=50 cluster labels reveal clear biological themes:
  - Cancer subtypes: breast (5.4K), prostate (2.9K), glioblastoma (3.6K), leukemia (6.5K)
  - Model organisms: arabidopsis (7.6K), drosophila (3.9K), yeast (4.7K), zebrafish (3.2K)
  - Cell biology: stem cells (6.3K), chromatin/epigenetics (8.8K), immune cells (3.9K)
  - Emerging: CRISPR (5.2K), microglia (4.9K), aging/senescence (2.1K)
- HDBSCAN clusters are more granular (348 clusters, 100-3,200 projects each)
- Several ENCODE-dominated clusters labeled "www / genome gov" (boilerplate descriptions)

#### Step 6: Temporal & Institutional Analysis (`python/08a_temporal_institutional.py`)

- **Pseudotime UMAP**: shows clear temporal gradient — older studies (microarray era) vs newer (sequencing era)
- **Emerging topics** (highest mean submission year): tumor immunity, CRISPR, microglia, aging/senescence, chromatin
- **Declining topics**: copy number arrays (mean 2014), small RNA/arabidopsis (mean 2016)
- **Country profiles**: USA projects spread across all clusters; China concentrated in cancer/genomics
- **Institution profiles**: ENCODE DCC forms distinct dense cluster; Stanford/Harvard spread broadly
- **Modality trends**: RNA-seq overtook microarray ~2015, ChIP-seq plateaued ~2018

### Notes & Potential Next Steps

- The ENCODE DCC boilerplate descriptions create noise clusters — could filter these for cleaner analysis
- HDBSCAN's 32% noise rate suggests many projects don't fit neatly into dense clusters — expected for a broad repository
- A biomedical-specific embedding model (BioSentVec, PubMedBERT) might improve cluster coherence for biology-specific terms
- LLM-based cluster summarization could provide richer labels than TF-IDF keywords
- NER + ontology mapping (MeSH, Disease Ontology, UBERON) was planned but not implemented — would enable structured queries
- The embedding matrix enables nearest-neighbor search for finding related studies
