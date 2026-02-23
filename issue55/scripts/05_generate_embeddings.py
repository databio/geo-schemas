#!/usr/bin/env python3
"""Step 3: Generate text embeddings for GEO series metadata.
Issue #55 — GEO Series-Level Semantic Meta Analysis

Uses sentence-transformers to embed the combined text field.
Processes in batches and saves to parquet.
"""

import os
import time
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

load_dotenv()

MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"
BATCH_SIZE = 512
INPUT_FILE = "../../data/geo_text_for_embedding.parquet"
OUTPUT_FILE = "../../data/geo_embeddings.parquet"
DEVICE = "mps"  # Apple Silicon GPU

print(f"Loading model: {MODEL_NAME} (device={DEVICE})")
model = SentenceTransformer(MODEL_NAME, device=DEVICE)
print(f"Model loaded. Embedding dimension: {model.get_sentence_embedding_dimension()}")

print(f"Loading {INPUT_FILE}...")
df = pd.read_parquet(INPUT_FILE)
texts = df["text"].tolist()
gse_ids = df["gse_id"].tolist()
print(f"Loaded {len(texts)} texts")

print(f"Generating embeddings (batch_size={BATCH_SIZE})...")
t0 = time.time()
embeddings = model.encode(
    texts,
    batch_size=BATCH_SIZE,
    show_progress_bar=True,
    normalize_embeddings=True,
)
elapsed = time.time() - t0
print(f"Embedding complete in {elapsed:.1f}s ({len(texts)/elapsed:.0f} texts/sec)")
print(f"Embedding shape: {embeddings.shape}")

# Save as parquet: gse_id + one column per embedding dimension
print(f"Saving to {OUTPUT_FILE}...")
emb_cols = {f"emb_{i}": embeddings[:, i] for i in range(embeddings.shape[1])}
out_df = pd.DataFrame({"gse_id": gse_ids, **emb_cols})
out_df.to_parquet(OUTPUT_FILE, index=False)
print(f"Done. Shape: {out_df.shape}")
