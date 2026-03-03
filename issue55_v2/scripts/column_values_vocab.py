import os
import re
import json
import zipfile
import pandas as pd
from collections import Counter
from tqdm import tqdm

# -------------------------
# Paths
# -------------------------
PEPS_DIR = os.path.abspath(os.path.join("..", "..", "data", "peps"))
SYNONYM_FILE = os.path.abspath(os.path.join("..", "..", "output", "synonym_clusters.csv"))
OUTPUT_FILE = os.path.abspath(os.path.join("..", "..", "output", "column_values_vocab.json"))

# -------------------------
# Columns of interest
# -------------------------
COLUMNS_OF_INTEREST = [
    "tissue",
    "cell_type",
    "cell_line",
    "strain",
    "antibody",
    "disease"
]

# -------------------------
# Helper: parse variant column names
# -------------------------
def parse_variants(variant_string):
    if pd.isna(variant_string):
        return []

    variants = []
    parts = variant_string.split(";")

    for part in parts:
        name = part.strip()
        name = re.sub(r"\s*\(.*?\)", "", name)  # remove "(counts)"
        if name:
            variants.append(name)

    return variants


# -------------------------
# Load synonym clusters
# -------------------------
syn_df = pd.read_csv(SYNONYM_FILE)

column_map = {col: set([col]) for col in COLUMNS_OF_INTEREST}

for _, row in syn_df.iterrows():
    canonical = row["canonical"]

    if canonical in COLUMNS_OF_INTEREST:
        variants = parse_variants(row.get("variants", ""))
        column_map[canonical].update(variants)

# -------------------------
# Vocabulary storage
# -------------------------
vocab = {col: Counter() for col in COLUMNS_OF_INTEREST}

# -------------------------
# Collect all GSE folders
# -------------------------
gse_folders = [
    f for f in os.listdir(PEPS_DIR)
    if os.path.isdir(os.path.join(PEPS_DIR, f)) and f.lower().startswith("gse")
]

# -------------------------
# Traverse folders with progress bar
# -------------------------
for gse_folder in tqdm(gse_folders, desc="Processing GSE folders"):

    gse_path = os.path.join(PEPS_DIR, gse_folder)

    zip_files = [
        f for f in os.listdir(gse_path)
        if f.endswith(".zip")
    ]

    for file in tqdm(zip_files, desc=f"{gse_folder}", leave=False):

        zip_path = os.path.join(gse_path, file)

        try:
            with zipfile.ZipFile(zip_path, "r") as z:

                sample_files = [
                    name for name in z.namelist()
                    if name.endswith("sample_table.csv")
                ]

                if not sample_files:
                    continue

                with z.open(sample_files[0]) as f:
                    df = pd.read_csv(f)

                df.columns = [c.strip() for c in df.columns]

                for canonical, variants in column_map.items():

                    matching_cols = [c for c in df.columns if c in variants]

                    for col in matching_cols:
                        for val in df[col].dropna():
                            val = str(val).strip()
                            if val:
                                vocab[canonical][val] += 1

        except Exception as e:
            tqdm.write(f"Error processing {zip_path}: {e}")

# -------------------------
# Convert Counter -> dict
# -------------------------
output_dict = {
    col: dict(vocab[col])
    for col in COLUMNS_OF_INTEREST
}

# -------------------------
# Write JSON
# -------------------------
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

with open(OUTPUT_FILE, "w") as f:
    json.dump(output_dict, f, indent=4)

print(f"\nVocabulary saved to: {OUTPUT_FILE}")