import json
import sys
import time
from pathlib import Path

import requests
import re
from tqdm import tqdm

HERE = Path(__file__).parent
VOCAB_PATH = HERE / "../../output/column_values_vocab.json"
OUTPUT_PATH = HERE / "../../output/onto_map.json"

OLS_SEARCH_URL = "https://www.ebi.ac.uk/ols4/api/search"

# Preferred ontology per column; None = search all loaded ontologies
# COLUMN_ONTOLOGY: dict[str, str | None] = {
#     "tissue": "uberon",
#     "cell_type": "cl",
#     "cell_line": "efo",
#     "strain": None,
#     "antibody": "pr",
#     "disease": "mondo",
# }


def _normalize(text: str) -> str:
    """Lowercase and collapse whitespace/hyphens/underscores for loose comparison."""
    return re.sub(r"[\s\-_]+", "", text.strip().lower())


def _query_ols(
    term: str,
    ontology: str | None,
    exact: bool,
    rows: int,
    retries: int = 3,
    backoff: float = 2.0,
) -> list[dict]:
    params: dict = {"q": term, "rows": rows, "exact": str(exact).lower()}
    if ontology:
        params["ontology"] = ontology
    for attempt in range(retries):
        try:
            resp = requests.get(OLS_SEARCH_URL, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json().get("response", {}).get("docs", [])
        except (requests.RequestException, ValueError):
            if attempt == retries - 1:
                raise
            time.sleep(backoff * (attempt + 1))
    return []


def check_ontology_term(
    term: str,
    ontology: str | None = None,
    rows: int = 50,
) -> dict:
    """
    Classify a term against OLS ontologies into one of three statuses.

    Status values
    -------------
    "exact"        : term is the canonical label of an ontology concept
    "mappable"     : term is a known synonym / alternate form of a concept,
                     or is close enough that OLS returns a confident hit
    "not_mappable" : no ontology concept found

    Parameters
    ----------
    term : str
        Input term to look up.
    ontology : str or None
        Restrict to a specific ontology short name (e.g. "efo", "cl", "uberon").
        None searches across all loaded ontologies.
    rows : int
        Max candidates retrieved per OLS query.

    Returns
    -------
    dict
        status        : "exact" | "mappable" | "not_mappable"
        match_type    : "label" | "synonym" | "fuzzy" | None
        matched_label : canonical ontology label of the best match, or None
        ontology_id   : e.g. "EFO:0000400", or None
        iri           : full term IRI, or None
    """
    term_norm = _normalize(term)

    # Pass 1: OLS server-side exact match — fast, hits only canonical labels
    for doc in _query_ols(term, ontology, exact=True, rows=rows):
        if _normalize(doc.get("label", "")) == term_norm:
            return {
                "status": "exact",
                "match_type": "label",
                "matched_label": doc.get("label"),
                "ontology_id": doc.get("obo_id"),
                "iri": doc.get("iri"),
            }

    # Pass 2: broad query — scan synonyms for non-standard / alternate forms
    broad_docs = _query_ols(term, ontology, exact=False, rows=rows)
    if not broad_docs:
        return {
            "status": "not_mappable",
            "match_type": None,
            "matched_label": None,
            "ontology_id": None,
            "iri": None,
        }

    for doc in broad_docs:
        synonyms = doc.get("synonym") or []
        if any(_normalize(syn) == term_norm for syn in synonyms):
            return {
                "status": "mappable",
                "match_type": "synonym",
                "matched_label": doc.get("label"),
                "ontology_id": doc.get("obo_id"),
                "iri": doc.get("iri"),
            }

    # OLS returned results but term didn't normalize-match any label or synonym
    best = broad_docs[0]
    return {
        "status": "mappable",
        "match_type": "fuzzy",
        "matched_label": best.get("label"),
        "ontology_id": best.get("obo_id"),
        "iri": best.get("iri"),
    }


DELAY = 0.1  # seconds between OLS requests


def main() -> None:
    vocab: dict[str, dict[str, int]] = json.loads(VOCAB_PATH.read_text())

    # Load existing results so we can resume interrupted runs
    results: dict[str, dict[str, dict]] = {}
    if OUTPUT_PATH.exists():
        results = json.loads(OUTPUT_PATH.read_text())
        print(f"Resuming from {OUTPUT_PATH} ({sum(len(v) for v in results.values())} terms already done).")

    for col, values in vocab.items():
        # ontology = COLUMN_ONTOLOGY.get(col)
        all_terms = list(values.keys())
        already_done = results.get(col, {})
        pending = [t for t in all_terms if t not in already_done]

        if not pending:
            print(f"[{col}] all {len(all_terms)} terms already mapped — skipping.")
            continue

        col_results = dict(already_done)
        status_counts: dict[str, int] = {}

        with tqdm(pending, desc=col, unit="term", initial=len(already_done), total=len(all_terms)) as pbar:
            for term in pbar:
                try:
                    mapping = check_ontology_term(term)
                except Exception as exc:
                    tqdm.write(f"ERROR [{col}] {term!r}: {exc}", file=sys.stderr)
                    mapping = {
                        "status": "error",
                        "match_type": None,
                        "matched_label": None,
                        "ontology_id": None,
                        "iri": None,
                    }

                col_results[term] = mapping
                status_counts[mapping["status"]] = status_counts.get(mapping["status"], 0) + 1
                pbar.set_postfix(status_counts)

                time.sleep(DELAY)

        results[col] = col_results

        # Write after each column so progress is preserved on interruption
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(results, indent=2, ensure_ascii=False))
        print(f"  Saved → {OUTPUT_PATH}")

    print("\nDone.")


if __name__ == "__main__":
    main()
