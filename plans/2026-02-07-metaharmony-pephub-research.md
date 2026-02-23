# Research: MetaHarmony, PEPhub, GEOfetch, and the Databio Metadata Ecosystem

**Date:** 2026-02-07

---

## 1. MetaHarmony R01 Grant

**Status: Not found in public records.**

I searched extensively across NIH Reporter (including direct API queries), web searches, the databio.org website, and Nathan Sheffield's publications. No grant or project specifically named "MetaHarmony" appears in any publicly indexed source.

Sheffield has three active/recent NIH grant lines at UVA:

| Grant | Title | Period | Institute |
|-------|-------|--------|-----------|
| R01-HG012558 | Novel methods for large-scale genomic interval comparison | 2022-08 to 2026-05 | NHGRI |
| R01-LM014012 | Continually Adaptive ML Platform for Personalized Biomedical Literature Curation | 2023-08 to 2027-04 | NLM |
| R35-GM128636 | A modular data analysis ecosystem using portable encapsulated projects | 2018-08 to 2023-07 (ended) | NIGMS |

The R35-GM128636 ("modular data analysis ecosystem using portable encapsulated projects") is the grant that directly funded PEPhub, GEOfetch, and the PEP ecosystem. The R01-HG012558 is about genomic interval comparison (BEDbase, region sets), not metadata harmonization per se.

**Possible explanations:** "MetaHarmony" may be an internal project name, a pending/unfunded proposal, or a name used informally for a sub-aim within one of the above grants. It is not a publicly searchable NIH grant title. Sub-Aim 1.3 could not be located.

---

## 2. PEPhub

### What it is

PEPhub is a **database, web interface, and API for editing, sharing, and validating biological sample metadata**. It is hosted at [pephub.databio.org](https://pephub.databio.org). Published in GigaScience (2024).

### Architecture

PEPhub consists of three major components:
- A **FastAPI web service** (backend)
- A **PostgreSQL database**
- The **PEPhubClient** Python package (CLI + programmatic access)

### Scale

The public instance contains **over 150,000 projects (PEPs)** derived from the Gene Expression Omnibus (GEO), with automated weekly updates via GEOfetch. This covers approximately 99% of GEO.

### Key Features

- **Natural language search** using sentence transformers and vector embeddings
- **Format conversion** to JSON, YAML, CSV, plain text
- **Metadata validation** via JSON Schema (through the eido tool)
- **GitHub authentication** with public/private access controls
- **Programmatic API** for pipeline integration
- **BEDMS integration** for metadata standardization (see below)

### Why it matters

PEPhub addresses a critical gap: while significant infrastructure exists for sharing biological *data*, relatively little addresses sharing *metadata* (the descriptions of samples, conditions, and experimental attributes). PEPhub makes GEO metadata programmatically accessible, searchable, and standardized.

### Funding

Supported by NIGMS R35-GM128636 and NHGRI R01-HG012558.

---

## 3. PEP (Portable Encapsulated Projects)

### What it is

PEP is a **community standard for organizing sample metadata**. It provides:
1. A **standardized metadata structure** (YAML config + CSV sample table)
2. A **metadata validation framework** (JSON Schema-based)
3. **Programmatic metadata modifiers** (amendments, imports, etc.)

### Format

A PEP consists of:
- A `_config.yaml` file (project-level attributes, sample modifiers)
- A `_annotation.csv` file (sample table: one row per sample, columns are attributes)
- Optional subsample tables for multi-valued attributes (e.g., multiple FASTQ files per sample)

### Ecosystem (PEPkit)

PEP is the core standard around which the **PEPkit** toolkit is built:
- **peppy** -- Python package for reading/writing PEPs
- **pepr** -- R package for PEPs
- **eido** -- Schema validation for PEPs
- **looper** -- Workflow execution using PEP metadata
- **geofetch** -- GEO/SRA to PEP converter
- **pipestat** -- Results tracking
- **PEPhub** -- Web repository for PEPs

---

## 4. GEOfetch

### What it does

GEOfetch is a **command-line tool that downloads sequencing data and metadata from GEO and SRA and converts the metadata into standardized PEP format**.

### Key capabilities

- Takes GEO accessions (GSE numbers) or SRA accessions as input
- Downloads raw data (FASTQ via SRA) and/or processed data from GEO
- Produces a standardized PEP: `_config.yaml` + `_annotation.csv`
- Combines samples from different projects into unified datasets
- Filters processed files by type and size before downloading
- Works as both CLI tool and Python API
- Available via pip and bioconda

### Relevance to metadata harmonization

GEOfetch is the tool that populates PEPhub's `geo/` namespace with 150,000+ projects. It extracts and restructures the complex, hierarchical GEO/SRA metadata into flat, standardized sample tables. However, it performs **structural** standardization (putting metadata into a consistent tabular format) rather than **semantic** standardization (mapping free-text attribute values to controlled vocabularies).

### Publication

Khoroshevskyi, LeRoy, Reuter, Sheffield. "GEOfetch: a command-line tool for downloading data and standardized metadata from GEO and SRA." Bioinformatics, 2023.

---

## 5. The Databio Lab and Metadata Standardization

### Lab overview

Nathan Sheffield leads the **databio lab** in the Department of Genome Sciences at the University of Virginia. The lab focuses on computational biology and bioinformatics with emphasis on cancer, epigenetics, development, and genomics. Sheffield is an Associate Professor with appointments across Genome Sciences, Biomedical Engineering, Biochemistry and Molecular Genetics, and the School of Data Science.

### Standards work

Sheffield co-leads several standards development projects in:
- **Global Alliance for Genomics and Health (GA4GH)** -- refget sequence collections standard
- **Research Data Alliance (RDA)**

### Key projects related to metadata

1. **PEP/PEPkit/PEPhub** -- The sample metadata standard and ecosystem (described above)
2. **BEDbase** -- A database and API for genomic interval (BED file) data with standardized metadata
3. **BEDMS** (BED Metadata Standardizer) -- Uses ML to predict standardized metadata attributes for BED files according to user-chosen schemas (ENCODE, FAIRTRACKS, BEDBASE)
4. **Refgenie / Refget Sequence Collections** -- A standard for identifying and managing reference genomes
5. **GEOfetch** -- GEO-to-PEP conversion (described above)

### BEDMS and the metadata standardization approach

BEDMS is particularly relevant as it demonstrates the lab's approach to metadata standardization via machine learning. Published as a bioRxiv preprint (2024), BEDMS:
- Trains models on existing standardized metadata to predict schema-compliant attributes
- Supports multiple schemas (ENCODE, FAIRTRACKS, BEDBASE)
- Is deployed on PEPhub as a web interface
- Addresses the problem that genomic data attributes do not follow uniform schemas, hindering discovery, interoperability, and reusability

---

## 6. The Broader Metadata Harmonization Problem in Genomics

### The core problem

GEO (Gene Expression Omnibus) is the largest public repository for functional genomics data, with hundreds of thousands of datasets. However:

- **GEO allows arbitrary metadata fields** -- submitters can create any column names they want
- **No validation of values** -- GEO does not enforce controlled vocabularies or ontologies
- **Inconsistent terminology** -- the same concept may appear as "cancer," "carcinoma," "tumor," "neoplasm," "Ca." across different submissions
- **Inconsistent structure** -- some submitters use one row per sample, others one row per file; column names vary wildly
- **Metadata quality is variable** -- records suffer from redundancy, incompleteness, and inconsistency

### Six challenges (from Sheffield et al., Frontiers in Genetics, 2023)

1. **Findability** -- metadata scattered across servers, embedded in PDFs/Excel, behind authorization barriers
2. **Distribution** -- ad hoc mechanisms, metadata bundled with restricted data despite FAIR principles
3. **Terminology** -- researchers don't consistently use ontologies; controlled terms require extra effort
4. **Structure** -- inconsistent formats, ambiguous representations
5. **Versioning** -- metadata treated as static despite being mutable
6. **Portability** -- analytical attributes (file paths, reference genomes) lose relevance when moved

### Why it matters

Without harmonized metadata, it is extremely difficult to:
- Perform large-scale meta-analyses across studies
- Build ML/AI models on public genomic data
- Find comparable datasets for reanalysis
- Integrate data across repositories

### Current approaches

- **Structural standardization**: Tools like GEOfetch convert messy formats into consistent tabular structures (PEP)
- **Semantic standardization**: Tools like BEDMS use ML to map free-text attributes to controlled vocabularies
- **LLM-based approaches**: Recent work (Verbitsky et al., 2025) uses fine-tuned GPT-2 to automatically map researcher annotations to ontology terms, achieving 96% accuracy for in-dictionary terms
- **Community standards**: PEP, ENCODE schemas, FAIRTRACKS, and others provide target schemas

---

## Summary of What Could Not Be Confirmed

- No grant or project specifically named "MetaHarmony" was found in NIH Reporter, web searches, or the databio lab website
- No "Sub-Aim 1.3" was located for any Sheffield grant
- The name may be internal, informal, pending, or not yet publicly indexed

---

## Sources

- [PEPkit documentation](https://pep.databio.org/)
- [PEPhub paper (GigaScience)](https://academic.oup.com/gigascience/article/doi/10.1093/gigascience/giae033/7712217)
- [PEPhub paper (PMC)](https://pmc.ncbi.nlm.nih.gov/articles/PMC10462087/)
- [GEOfetch GitHub](https://github.com/pepkit/geofetch)
- [GEOfetch paper (PMC)](https://pmc.ncbi.nlm.nih.gov/articles/PMC9982356/)
- [Challenges to sharing sample metadata (Frontiers in Genetics)](https://www.frontiersin.org/journals/genetics/articles/10.3389/fgene.2023.1154198/full)
- [BEDMS preprint (bioRxiv)](https://www.biorxiv.org/content/10.1101/2024.09.18.613791v1)
- [Databio lab website](https://databio.org/)
- [PEPhub GEO namespace](https://pephub.databio.org/geo)
- NIH Reporter API (queried directly for Sheffield grants)
