# Issue #65: PEPhub Schema Diversity Analysis

## Context

The MetaHarmony R01 proposes ML architectures for automated metadata harmonization. It addresses two core problems: **schema mapping** (aligning attribute names across datasets) and **ontology mapping** (standardizing values to controlled vocabularies). The `geo` namespace in PEPhub contains ~229K GEO-derived PEPs, each with its own sample table columns. The diversity of those column names *is* the schema mapping problem.

Preliminary parsing of the archive reveals **61,803 unique column names** across 229,101 projects, with a median of 44 columns per project. Common concepts like "sample name", "genome", and "tissue" appear under dozens of variant names. Quantifying this directly supports Sub-Aim 1.3 (benchmark dataset for schema mapping evaluation).

**Source**: https://github.com/databio/lab.databio.org/discussions/65

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
| `series_contributor` | string | Authors ("+"-delimited) |
| `series_contact_country` | string | Country of submitting institution |
| `series_contact_institute` | string | Submitting institution name |
| `series_submission_date` | string | Submission date (e.g. "Jun 13 2017") |
| `series_platform_id` | string | GPL platform ID |
| `series_overall_design` | string | Experimental design description |
| `description` | string | PEPhub project description (has GEO link + title) |
| `pep_version` | string | PEP spec version |
| `column_names_json` | string | **JSON array of sample table column names** — this is the key field for #65 |

### Key field for this analysis

`column_names_json` contains a JSON array of all column names from that project's sample table. Example:

```json
["sample_name","protocol","organism","read_type","tissue","assembly","cell_type","chip_antibody","genome_build"]
```

Parse with `jsonlite::fromJSON()` in R.

### R packages needed

```r
install.packages(c("arrow", "data.table", "jsonlite", "ggplot2", "stringdist"))
```

## Current Status

- [x] Downloaded and extracted GEO namespace archive (229,101 PEPs)
- [x] Parsed all PEPs into `data/geo_metadata.parquet` (163 MB)
- [x] Column frequency analysis
- [x] GEO-standard vs user-defined classification
- [x] Synonym clustering
- [x] Publication figures
- [x] Benchmark mapping file

## Plan: Next Steps

### Step 1: Column Frequency Analysis
- Explode `column_names_json` to one row per column per project
- Count frequency of each unique column name
- Characterize the distribution: how many columns appear in >50% of projects vs singletons?
- Separate GEO-standard columns (e.g., `sample_name`, `sample_geo_accession`) from user-defined ones

**Output**: `data/column_frequencies.parquet`, frequency distribution summary stats

### Step 2: Identify GEO-Standard vs User-Defined Columns
- GEO-standard columns are injected by GEOfetch and appear in nearly every PEP (e.g., `sample_name`, `sample_geo_accession`, `sample_type`, `sample_platform_id`)
- User-defined columns are what submitters added and represent the real schema diversity
- Split columns into these two categories using a frequency threshold or known GEO field list

**Output**: Annotated column list with `is_standard` flag

### Step 3: Synonym Clustering
- For user-defined columns, group names that refer to the same concept
- Approach:
  1. Normalize: lowercase, strip whitespace/punctuation
  2. String similarity (Jaro-Winkler, cosine on character n-grams) for candidate pairs
  3. Manual/LLM-assisted review of top clusters
- Target: synonym clusters for top 20-30 concepts (tissue, genotype, treatment, cell type, age, sex, disease, etc.)

**Output**: `data/column_synonyms.json` mapping canonical names to variant lists

### Step 4: Publication Figures
1. **Long-tail distribution**: column usage frequency (log scale), highlighting the gap between common and rare columns
2. **Synonym cluster network/heatmap**: for top concepts, show how many variant names exist
3. **Schema complexity histogram**: columns-per-PEP distribution
4. **Harmonization gap**: for top 10 concepts, fraction of PEPs using "standard" name vs variants

### Step 5: Benchmark Dataset
- Curate `column_synonyms.json` as ground truth for schema mapping evaluation
- Format: `{"canonical_name": ["variant1", "variant2", ...]}`
- This feeds directly into MetaHarmony Sub-Aim 1.3

---

## Implementation Log (2026-02-07)

### Environment

- **Python 3.13.2**: venv at `.venv/`, managed with `uv`
- **Python packages**: `pyarrow`, `pandas`, `numpy`, `matplotlib`, `rapidfuzz`, `scipy`, `scikit-learn`

### Project Layout (post-implementation)

```
geo-schemas/
├── python/
│   ├── 03b_column_frequency.py        # Step 1
│   ├── 04b_standard_vs_user.py        # Step 2
│   ├── 05b_synonym_clustering.py      # Step 3
│   ├── 06b_publication_figures.py     # Step 4
│   └── 07b_benchmark_dataset.py       # Step 5
├── data/
│   ├── geo_metadata.parquet           # Input (163 MB, 229,101 rows)
│   ├── column_frequencies.parquet     # Step 1 output
│   ├── column_classifications.parquet # Step 2 output
│   ├── user_defined_columns.parquet   # Step 2 output (user-defined only)
│   ├── column_synonyms.json           # Step 3 output (simple format)
│   ├── column_synonyms_full.json      # Step 3 output (with frequencies)
│   ├── schema_mapping_benchmark.json  # Step 5 output (canonical -> variants)
│   └── schema_mapping_benchmark_annotated.json  # Step 5 (with frequencies)
├── output/issue65/
│   ├── column_frequencies.csv
│   ├── column_classifications.csv
│   ├── user_defined_columns.csv
│   ├── frequency_summary.json
│   ├── synonym_clusters.csv
│   ├── unassigned_columns.csv
│   ├── benchmark_summary.csv
│   ├── longtail_distribution.png      # Figure 1
│   ├── synonym_clusters_top20.png     # Figure 2
│   ├── schema_complexity_histogram.png # Figure 3
│   └── harmonization_gap.png          # Figure 4
```

### Step-by-Step Results

#### Step 1: Column Frequency Analysis (`python/03b_column_frequency.py`)

- 229,101 projects, 10,306,004 column-project pairs
- **61,803 unique column names** (confirms the preliminary count)
- Mean 45.0 columns per project, median 44
- 28 columns appear in >=90% of projects (GEO infrastructure)
- 35 columns in 10-90% range (common user fields)
- **40,362 singletons (65.3%)** — massive long tail
- Top user-defined columns: tissue (37.8%), genome_build (35.5%), cell_type (32.0%)

#### Step 2: GEO-Standard vs User-Defined (`python/04b_standard_vs_user.py`)

- Classification via known GEO/GEOfetch prefixes (`sample_*`, `srr`, `srx`, etc.)
- 732 standard columns, **61,071 user-defined columns**
- Top standard columns are all >90% prevalence (contact info, platform, accession)
- Clear separation: most frequent user-defined (tissue, 37.8%) well below standard floor

#### Step 3: Synonym Clustering (`python/05b_synonym_clustering.py`)

- Concept-based matching: 62 manually defined seed concepts, token-overlap + fuzzy matching
- Initial connected-component approach failed (mega-cluster of 525 columns via transitive chaining)
- Seed-based approach: 49 well-separated clusters, 213 columns assigned, 564 unassigned
- Key findings:
  - `antibody`: 25 variant names (chip_antibody, rip_antibody, ip_antibody, antibody_vendor, etc.)
  - `treatment`: 16 variants (treatment_time, treatment_dose, drug_treatment, etc.)
  - `tissue`: 12 variants (tissue_type, tissue_source, tissue_origin, etc.)
  - `sex`/`gender`: a clean synonym pair (30K + 17K projects)
  - `developmental_stage`/`dev_stage`/`development_stage`/`developemental_stage`: typo + abbreviation variants

#### Step 4: Publication Figures (`python/06b_publication_figures.py`)

1. **Long-tail distribution** — log-log rank-frequency plot showing the extreme skew: a handful of columns in >30% of projects, then rapid dropoff to 40K singletons
2. **Synonym clusters bar chart** — top 20 concepts by variant count; antibody (25) and treatment (16) lead
3. **Schema complexity histogram** — tight distribution around median 44 columns, right tail to 100+
4. **Harmonization gap** — most concepts have >85% canonical name usage, but antibody (47%) and replicate (45%) are split roughly 50/50 with variants

#### Step 5: Benchmark Dataset (`python/07b_benchmark_dataset.py`)

- Merged overlapping clusters (sex/gender, disease/disease_state, etc.)
- Filtered out generic/infrastructure column names
- **38 canonical concepts, 190 total unique column names**
- Format: `{"canonical_name": ["variant1", "variant2", ...]}`
- Feeds directly into MetaHarmony Sub-Aim 1.3 schema mapping evaluation

### Key Takeaways for the Grant

1. **Scale of the problem**: 61,803 unique column names across 229K GEO projects — the schema mapping challenge is enormous
2. **Long tail dominance**: 65% of column names appear in exactly 1 project — one-off naming is the norm
3. **Synonym prevalence**: Even the top 10 biological concepts have 4-25 variant names each
4. **Harmonization gap**: For some concepts (antibody, replicate), more than half of projects use non-standard variant names
5. **Benchmark ready**: 38 concepts with 190 mapped variant names as ground truth for ML schema mapping evaluation
