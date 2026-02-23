#!/usr/bin/env python3
"""Fix era color and add institution-by-era UMAP plots."""

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

OUTPUT_DIR = Path('../output')

print("Loading data...")
clusters = pd.read_parquet(OUTPUT_DIR / 'umap_clusters.parquet')
meta = pd.read_parquet('../../data/geo_metadata.parquet')
df = clusters.merge(meta, on='gse_id')
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
era_colors = {'2001-2010': '#440154', '2011-2015': '#31688e', '2016-2019': '#35b779', '2020-2024': '#d62728'}

# ============================================================
# 1. Fix UMAP by era (red instead of yellow for 2020-2024)
# ============================================================
print("Regenerating UMAP by era...")
fig, axes = plt.subplots(2, 2, figsize=(16, 14))
for ax, era in zip(axes.flat, era_order):
    mask = df['era'] == era
    ax.scatter(df['umap_1'], df['umap_2'], s=0.1, c='lightgrey', alpha=0.05)
    ax.scatter(df.loc[mask, 'umap_1'], df.loc[mask, 'umap_2'],
               s=0.3, c=era_colors[era], alpha=0.3)
    ax.set_title(f'{era} (n={mask.sum():,})', fontsize=14)
    ax.set_xticks([]); ax.set_yticks([])
plt.suptitle('GEO Projects by Era on UMAP', fontsize=16, y=1.01)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'umap_by_era.png', dpi=200, bbox_inches='tight')
plt.close()
print("  Saved umap_by_era.png")

# ============================================================
# 2. Top 5 institutions per era - rows=institutions, cols=eras
# ============================================================
print("\nPlotting top 5 institutions per era as subplots...")

# Exclude ENCODE DCC since it dominates
df_no_encode = df[df['series_contact_institute'] != 'ENCODE DCC']

# Get union of top 5 institutions from each era (preserving order of first appearance)
all_top = []
for era in era_order:
    era_df = df_no_encode[df_no_encode['era'] == era]
    top5 = era_df['series_contact_institute'].value_counts().head(5).index.tolist()
    for inst in top5:
        if inst not in all_top:
            all_top.append(inst)

# Precompute ranks for each institution in each era
era_ranks = {}
for era in era_order:
    era_df = df_no_encode[df_no_encode['era'] == era]
    ranked = era_df['series_contact_institute'].value_counts()
    era_ranks[era] = {inst: rank + 1 for rank, inst in enumerate(ranked.index)}

n_inst = len(all_top)
fig, axes = plt.subplots(n_inst, 4, figsize=(16, n_inst * 3))
colors = plt.cm.tab20.colors  # More colors for 12 institutions

for row_idx, inst in enumerate(all_top):
    inst_color = colors[row_idx % len(colors)]

    for col_idx, era in enumerate(era_order):
        ax = axes[row_idx, col_idx]

        # Background points
        ax.scatter(df['umap_1'], df['umap_2'], s=0.1, c='lightgrey', alpha=0.05)

        # Plot this institution's projects for this era
        mask = (df['series_contact_institute'] == inst) & (df['era'] == era)
        if mask.sum() > 0:
            ax.scatter(df.loc[mask, 'umap_1'], df.loc[mask, 'umap_2'],
                       s=1.5, c=[inst_color], alpha=0.6)

        ax.set_xticks([]); ax.set_yticks([])

        # Add era label on top row
        if row_idx == 0:
            ax.set_title(era, fontsize=11, fontweight='bold')

        # Add institution label on leftmost column
        if col_idx == 0:
            short_name = inst[:22] + '...' if len(inst) > 22 else inst
            ax.set_ylabel(short_name, fontsize=8, fontweight='bold')

        # Add count and rank in corner
        rank = era_ranks[era].get(inst, '-')
        rank_str = f'#{rank}' if isinstance(rank, int) else rank
        ax.text(0.95, 0.95, f'n={mask.sum()}\n{rank_str}', transform=ax.transAxes,
                fontsize=7, ha='right', va='top')

plt.suptitle(f'Institutions on UMAP by Era (union of top 5 per era, {n_inst} total, excluding ENCODE DCC)',
             fontsize=13, y=1.005)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / 'report' / 'umap_institutions_by_era.png', dpi=200, bbox_inches='tight')
plt.close()
print(f"  Saved umap_institutions_by_era.png ({n_inst} institutions)")

# Print the top 10 for each era
print("\nTop 10 institutions per era:")
for era in era_order:
    era_df = df_no_encode[df_no_encode['era'] == era]
    top10 = era_df['series_contact_institute'].value_counts().head(10)
    print(f"\n  {era}:")
    for inst, cnt in top10.items():
        print(f"    {inst}: {cnt}")

print("\nDone.")
