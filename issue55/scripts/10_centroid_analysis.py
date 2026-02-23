#!/usr/bin/env python3
"""Centroid trajectory analysis across pseudotime."""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from adjustText import adjust_text
from pathlib import Path

OUTPUT_DIR = Path('../output')
REPORT_DIR = OUTPUT_DIR / 'report'

print("Loading data...")
embeddings = pd.read_parquet('../../data/geo_embeddings.parquet')
meta = pd.read_parquet('../../data/geo_metadata.parquet')
umap_df = pd.read_parquet(OUTPUT_DIR / 'umap_clusters.parquet')

# Merge all data
df = embeddings.merge(meta, on='gse_id').merge(umap_df[['gse_id', 'umap_1', 'umap_2']], on='gse_id')
df['submission_date'] = pd.to_datetime(df['series_submission_date'], format='%b %d %Y', errors='coerce')
df['year'] = df['submission_date'].dt.year

def assign_era(y):
    if pd.isna(y): return None
    if y <= 2010: return '2001-2010'
    if y <= 2015: return '2011-2015'
    if y <= 2019: return '2016-2019'
    return '2020-2024'

df['era'] = df['year'].apply(assign_era)
era_order = ['2001-2010', '2011-2015', '2016-2019', '2020-2024']

# Get embedding columns
emb_cols = [c for c in df.columns if c.startswith('emb_')]
print(f"  {len(df)} projects, {len(emb_cols)} embedding dimensions")

# ============================================================
# 1. Country centroids over time
# ============================================================
print("\nComputing country centroids...")

top_countries = ['USA', 'China', 'Germany', 'Japan', 'United Kingdom', 'France', 'Canada', 'Australia']

# Compute centroids in 768-dim space, then get UMAP coordinates
country_centroids = []
for country in top_countries:
    for era in era_order:
        mask = (df['series_contact_country'] == country) & (df['era'] == era)
        if mask.sum() < 10:
            continue
        # Centroid in embedding space
        emb_centroid = df.loc[mask, emb_cols].mean().values
        # Centroid in UMAP space (for visualization)
        umap_centroid = df.loc[mask, ['umap_1', 'umap_2']].mean().values
        country_centroids.append({
            'country': country,
            'era': era,
            'n': mask.sum(),
            'umap_1': umap_centroid[0],
            'umap_2': umap_centroid[1]
        })

country_df = pd.DataFrame(country_centroids)
print(f"  Computed {len(country_df)} country-era centroids")

# Plot country trajectories as subplots
fig, axes = plt.subplots(2, 4, figsize=(20, 10))
colors = plt.cm.tab10.colors

for idx, country in enumerate(top_countries):
    ax = axes.flat[idx]
    cdf = country_df[country_df['country'] == country].sort_values('era')

    # Background points
    ax.scatter(df['umap_1'], df['umap_2'], s=0.1, c='lightgrey', alpha=0.05)

    if len(cdf) >= 2:
        color = colors[idx % len(colors)]

        # Plot trajectory with arrows
        for i in range(len(cdf) - 1):
            row1 = cdf.iloc[i]
            row2 = cdf.iloc[i + 1]
            ax.annotate('', xy=(row2['umap_1'], row2['umap_2']),
                        xytext=(row1['umap_1'], row1['umap_2']),
                        arrowprops=dict(arrowstyle='-|>', color=color, lw=1.0, alpha=0.8, mutation_scale=7))

        # Plot centroid points
        ax.scatter(cdf['umap_1'], cdf['umap_2'], s=8, c=[color],
                   edgecolors='black', linewidths=0.4, alpha=0.9, zorder=10)

    ax.set_title(country, fontsize=11, fontweight='bold')
    ax.set_xticks([]); ax.set_yticks([])

plt.suptitle('Country Centroid Trajectories Across Eras (2001-2010 → 2020-2024)', fontsize=14, y=1.02)
plt.tight_layout()
plt.savefig(REPORT_DIR / 'centroid_countries.png', dpi=200, bbox_inches='tight')
plt.close()
print("  Saved centroid_countries.png")

# ============================================================
# 2. Compute centroid displacement distances
# ============================================================
print("\nComputing centroid displacements...")

displacements = []
for country in top_countries:
    cdf = country_df[country_df['country'] == country].sort_values('era')
    if len(cdf) < 2:
        continue

    # Total displacement from first to last era
    first = cdf.iloc[0]
    last = cdf.iloc[-1]
    total_dist = np.sqrt((last['umap_1'] - first['umap_1'])**2 + (last['umap_2'] - first['umap_2'])**2)

    displacements.append({
        'country': country,
        'total_displacement': total_dist,
        'n_first': first['n'],
        'n_last': last['n']
    })

disp_df = pd.DataFrame(displacements).sort_values('total_displacement', ascending=False)
print("\nCountry centroid displacements (UMAP units):")
for _, row in disp_df.iterrows():
    print(f"  {row['country']}: {row['total_displacement']:.2f}")

# ============================================================
# 3. Topic (k=50 cluster) centroids over time
# ============================================================
print("\nComputing topic centroids...")

# Load cluster labels
cluster_labels = pd.read_csv(OUTPUT_DIR / 'cluster_labels_k50.csv')
cluster_to_label = dict(zip(cluster_labels['cluster'], cluster_labels['label']))

# Merge cluster assignments
clusters_full = pd.read_parquet(OUTPUT_DIR / 'umap_clusters.parquet')
df = df.merge(clusters_full[['gse_id', 'kmeans_50']], on='gse_id', how='left')

# Get top 10 clusters by size
top_clusters = df['kmeans_50'].value_counts().head(10).index.tolist()

topic_centroids = []
for cluster in top_clusters:
    for era in era_order:
        mask = (df['kmeans_50'] == cluster) & (df['era'] == era)
        if mask.sum() < 10:
            continue
        umap_centroid = df.loc[mask, ['umap_1', 'umap_2']].mean().values
        topic_centroids.append({
            'cluster': cluster,
            'label': cluster_to_label.get(cluster, f'Cluster {cluster}'),
            'era': era,
            'n': mask.sum(),
            'umap_1': umap_centroid[0],
            'umap_2': umap_centroid[1]
        })

topic_df = pd.DataFrame(topic_centroids)
print(f"  Computed {len(topic_df)} topic-era centroids")

# Plot topic trajectories as subplots
fig, axes = plt.subplots(2, 5, figsize=(25, 10))
colors = plt.cm.tab10.colors

for idx, cluster in enumerate(top_clusters):
    ax = axes.flat[idx]
    tdf = topic_df[topic_df['cluster'] == cluster].sort_values('era')

    # Background points
    ax.scatter(df['umap_1'], df['umap_2'], s=0.1, c='lightgrey', alpha=0.05)

    label = cluster_to_label.get(cluster, f'Cluster {cluster}')
    short_label = ' / '.join(label.split(' / ')[:2])

    if len(tdf) >= 2:
        color = colors[idx % len(colors)]

        # Plot trajectory with arrows
        for j in range(len(tdf) - 1):
            row1 = tdf.iloc[j]
            row2 = tdf.iloc[j + 1]
            ax.annotate('', xy=(row2['umap_1'], row2['umap_2']),
                        xytext=(row1['umap_1'], row1['umap_2']),
                        arrowprops=dict(arrowstyle='-|>', color=color, lw=1.0, alpha=0.8, mutation_scale=7))

        # Plot centroid points
        ax.scatter(tdf['umap_1'], tdf['umap_2'], s=8, c=[color],
                   edgecolors='black', linewidths=0.4, alpha=0.9, zorder=10)

    ax.set_title(short_label, fontsize=10, fontweight='bold')
    ax.set_xticks([]); ax.set_yticks([])

plt.suptitle('Topic Centroid Trajectories Across Eras (Top 10 Clusters)', fontsize=14, y=1.02)
plt.tight_layout()
plt.savefig(REPORT_DIR / 'centroid_topics.png', dpi=200, bbox_inches='tight')
plt.close()
print("  Saved centroid_topics.png")

# ============================================================
# 4. Compute topic displacement distances
# ============================================================
print("\nTopic centroid displacements (UMAP units):")

topic_displacements = []
for cluster in top_clusters:
    tdf = topic_df[topic_df['cluster'] == cluster].sort_values('era')
    if len(tdf) < 2:
        continue

    first = tdf.iloc[0]
    last = tdf.iloc[-1]
    total_dist = np.sqrt((last['umap_1'] - first['umap_1'])**2 + (last['umap_2'] - first['umap_2'])**2)

    label = cluster_to_label.get(cluster, f'Cluster {cluster}')
    short_label = ' / '.join(label.split(' / ')[:2])

    topic_displacements.append({
        'cluster': cluster,
        'label': short_label,
        'total_displacement': total_dist
    })
    print(f"  {short_label}: {total_dist:.2f}")

# ============================================================
# 5. Save centroid stats
# ============================================================
import json

# Convert numpy types to Python types for JSON serialization
def to_python(obj):
    if isinstance(obj, dict):
        return {k: to_python(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [to_python(v) for v in obj]
    elif hasattr(obj, 'item'):  # numpy scalar
        return obj.item()
    return obj

centroid_stats = {
    'country_displacements': to_python(disp_df.to_dict('records')),
    'topic_displacements': to_python(sorted(topic_displacements, key=lambda x: -x['total_displacement']))
}

with open(OUTPUT_DIR / 'centroid_stats.json', 'w') as f:
    json.dump(centroid_stats, f, indent=2)
print(f"\nSaved centroid_stats.json")

print("\nDone.")
