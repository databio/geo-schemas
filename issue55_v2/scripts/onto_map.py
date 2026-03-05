import json
import sys
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm import tqdm


HERE = Path(__file__).parent
VOCAB_PATH = HERE / "../../output/column_values_vocab.json"
OUTPUT_PATH = HERE / "../../output/onto_map.json"

OLS_SEARCH_URL = "https://www.ebi.ac.uk/ols4/api/search"

COLUMN_ONTOLOGY: dict[str, str | None] = {
    "tissue": "uberon,bto,ma,emapa,fma,hra,ccf,caro,ehdaa2",
    "cell_type": "cl,pcl,hcao,bto",
    "cell_line": "clo,bto,efo",
    "strain": "rs,ncbitaxon,vbo",
    "antibody": "obi,efo,ncit",
    "disease": "doid,mondo,ado,epio,cvdo,scdo,cido,ido,idomal,idocovid19,mfomd,ordo,ogms,ncit,htn,symp,hp"
}

MAX_WORKERS = 16
ROWS = 50


def _normalize(text: str) -> str:
    return re.sub(r"[\s\-_]+", "", text.strip().lower())


def _query_ols(session: requests.Session, term: str, ontology: str | None) -> list[dict]:
    params = {"q": term, "rows": ROWS, "exact": "false"}
    if ontology:
        params["ontology"] = ontology

    resp = session.get(OLS_SEARCH_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("response", {}).get("docs", [])


def check_ontology_term(session: requests.Session, term: str, ontology: str | None):
    term_norm = _normalize(term)

    docs = _query_ols(session, term, ontology)

    if not docs:
        return {
            "status": "not_mappable",
            "match_type": None,
            "matched_label": None,
            "ontology_id": None,
            "iri": None,
        }

    # label match
    for doc in docs:
        if _normalize(doc.get("label", "")) == term_norm:
            return {
                "status": "exact",
                "match_type": "label",
                "matched_label": doc.get("label"),
                "ontology_id": doc.get("obo_id"),
                "iri": doc.get("iri"),
            }

    # synonym match
    for doc in docs:
        synonyms = doc.get("synonym") or []
        if any(_normalize(s) == term_norm for s in synonyms):
            return {
                "status": "mappable",
                "match_type": "synonym",
                "matched_label": doc.get("label"),
                "ontology_id": doc.get("obo_id"),
                "iri": doc.get("iri"),
            }

    # fuzzy
    best = docs[0]
    return {
        "status": "mappable",
        "match_type": "fuzzy",
        "matched_label": best.get("label"),
        "ontology_id": best.get("obo_id"),
        "iri": best.get("iri"),
    }


def worker(session, col, term, ontology):
    try:
        mapping = check_ontology_term(session, term, ontology)
    except Exception as exc:
        tqdm.write(f"ERROR [{col}] {term!r}: {exc}", file=sys.stderr)
        mapping = {
            "status": "error",
            "match_type": None,
            "matched_label": None,
            "ontology_id": None,
            "iri": None,
        }

    return col, term, mapping


def main():
    vocab: dict[str, dict[str, int]] = json.loads(VOCAB_PATH.read_text())

    results: dict[str, dict[str, dict]] = {}
    if OUTPUT_PATH.exists():
        results = json.loads(OUTPUT_PATH.read_text())
        print(f"Resuming from {OUTPUT_PATH}")

    session = requests.Session()

    tasks = []
    for col, values in vocab.items():
        ontology = COLUMN_ONTOLOGY.get(col)
        results.setdefault(col, {})

        for term in values:
            if term not in results[col]:
                tasks.append((col, term, ontology))

    print(f"Total terms to process: {len(tasks)}")

    status_counts = {}

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [
            executor.submit(worker, session, col, term, ontology)
            for col, term, ontology in tasks
        ]

        for future in tqdm(as_completed(futures), total=len(futures)):
            col, term, mapping = future.result()

            results[col][term] = mapping
            status_counts[mapping["status"]] = status_counts.get(mapping["status"], 0) + 1

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(results, indent=2, ensure_ascii=False))

    print("\nStatus counts:", status_counts)
    print("Saved →", OUTPUT_PATH)


if __name__ == "__main__":
    main()
