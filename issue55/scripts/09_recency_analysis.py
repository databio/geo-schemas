#!/usr/bin/env python3
"""Deep analysis of what characterizes newer GEO submissions.
Issue #55 — GEO Series-Level Semantic Meta Analysis

Examines research topics, countries, institutions, modalities, and organisms
stratified by submission era, with UMAP visualizations.
"""

import json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from pathlib import Path

OUTPUT_DIR = Path("../output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Load and merge ---
print("Loading data...")
clusters = pd.read_parquet(OUTPUT_DIR / "umap_clusters.parquet")
meta = pd.read_parquet("../../data/geo_metadata.parquet")
df = clusters.merge(meta, on="gse_id")
df["submission_date"] = pd.to_datetime(df["series_submission_date"], format="%b %d %Y", errors="coerce")
df["year"] = df["submission_date"].dt.year

# Define eras
def assign_era(y):
    if pd.isna(y): return None
    if y <= 2010: return "2001-2010"
    if y <= 2015: return "2011-2015"
    if y <= 2019: return "2016-2019"
    return "2020-2024"

df["era"] = df["year"].apply(assign_era)
era_order = ["2001-2010", "2011-2015", "2016-2019", "2020-2024"]
era_colors = {"2001-2010": "#440154", "2011-2015": "#31688e", "2016-2019": "#35b779", "2020-2024": "#d62728"}

print(f"Era counts:")
for era in era_order:
    n = (df["era"] == era).sum()
    print(f"  {era}: {n:,}")

# ============================================================
# 1. UMAP by era (4-panel)
# ============================================================
print("\nPlotting UMAP by era...")
fig, axes = plt.subplots(2, 2, figsize=(16, 14))
for ax, era in zip(axes.flat, era_order):
    mask = df["era"] == era
    ax.scatter(df["umap_1"], df["umap_2"], s=0.1, c="lightgrey", alpha=0.05)
    ax.scatter(df.loc[mask, "umap_1"], df.loc[mask, "umap_2"],
               s=0.3, c=era_colors[era], alpha=0.3)
    ax.set_title(f"{era} (n={mask.sum():,})", fontsize=14)
    ax.set_xticks([]); ax.set_yticks([])
plt.suptitle("GEO Projects by Era on UMAP", fontsize=16, y=1.01)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "umap_by_era.png", dpi=200, bbox_inches="tight")
plt.close()
print("  Saved umap_by_era.png")

# ============================================================
# 2. Modality shifts by era
# ============================================================
print("\nAnalyzing modality by era...")
def simplify_type(t):
    if pd.isna(t): return "Other"
    t = str(t).lower()
    if "expression" in t and "high throughput" in t: return "RNA-seq"
    elif "expression" in t and "array" in t: return "Microarray"
    elif "binding" in t or "chip" in t: return "ChIP-seq"
    elif "methylation" in t: return "Methylation"
    elif "non-coding" in t: return "ncRNA"
    elif "variation" in t or "snp" in t: return "Genomic variation"
    else: return "Other"

df["modality"] = df["series_type"].apply(simplify_type)
mod_era = df.groupby(["era", "modality"]).size().unstack(fill_value=0)
mod_era_pct = mod_era.div(mod_era.sum(axis=1), axis=0) * 100
mod_era_pct = mod_era_pct.reindex(era_order)

fig, ax = plt.subplots(figsize=(12, 6))
mod_era_pct.plot(kind="bar", stacked=True, ax=ax, colormap="Set2")
ax.set_title("Modality Composition by Era", fontsize=14)
ax.set_xlabel(""); ax.set_ylabel("% of Projects")
ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
ax.set_xticklabels(era_order, rotation=0)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "modality_by_era.png", dpi=150, bbox_inches="tight")
plt.close()
print("  Saved modality_by_era.png")

# Print modality stats
print("\n  Modality % by era:")
print(mod_era_pct.round(1).to_string())

# ============================================================
# 3. Country shifts by era
# ============================================================
print("\nAnalyzing country representation by era...")
top_countries = df["series_contact_country"].value_counts().head(10).index.tolist()
country_era = df[df["series_contact_country"].isin(top_countries)].groupby(
    ["era", "series_contact_country"]).size().unstack(fill_value=0)
country_era_pct = country_era.div(country_era.sum(axis=1), axis=0) * 100
country_era_pct = country_era_pct.reindex(era_order)

fig, ax = plt.subplots(figsize=(12, 6))
country_era_pct[["USA", "China", "Germany", "Japan", "United Kingdom",
                  "Canada", "France"]].plot(kind="bar", ax=ax, width=0.8)
ax.set_title("Top Country Representation by Era (among top 10)", fontsize=14)
ax.set_xlabel(""); ax.set_ylabel("% of Projects (within top 10 countries)")
ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
ax.set_xticklabels(era_order, rotation=0)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "country_by_era.png", dpi=150, bbox_inches="tight")
plt.close()
print("  Saved country_by_era.png")

print("\n  Country % by era (top 7):")
print(country_era_pct[["USA", "China", "Germany", "Japan", "United Kingdom",
                         "Canada", "France"]].round(1).to_string())

# ============================================================
# 4. Organism shifts by era
# ============================================================
print("\nAnalyzing organism representation by era...")
top_orgs = ["Homo sapiens", "Mus musculus", "Drosophila melanogaster",
            "Arabidopsis thaliana", "Rattus norvegicus"]
org_era = df[df["series_organism"].isin(top_orgs)].groupby(
    ["era", "series_organism"]).size().unstack(fill_value=0)
org_era_pct = org_era.div(org_era.sum(axis=1), axis=0) * 100
org_era_pct = org_era_pct.reindex(era_order)

fig, ax = plt.subplots(figsize=(12, 6))
org_era_pct.plot(kind="bar", ax=ax, width=0.8, colormap="Dark2")
ax.set_title("Top Organism Representation by Era", fontsize=14)
ax.set_xlabel(""); ax.set_ylabel("% of Projects (among top 5 organisms)")
ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
ax.set_xticklabels(era_order, rotation=0)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "organism_by_era.png", dpi=150, bbox_inches="tight")
plt.close()
print("  Saved organism_by_era.png")

print("\n  Organism % by era:")
print(org_era_pct.round(1).to_string())

# ============================================================
# 5. Institution emergence by era
# ============================================================
print("\nAnalyzing institutional shifts...")
# Top institutions per era (excluding ENCODE DCC)
inst_no_encode = df[df["series_contact_institute"] != "ENCODE DCC"]
for era in era_order:
    era_df = inst_no_encode[inst_no_encode["era"] == era]
    top = era_df["series_contact_institute"].value_counts().head(5)
    print(f"\n  Top 5 institutions in {era}:")
    for inst, cnt in top.items():
        print(f"    {inst}: {cnt}")

# ============================================================
# 6. Fastest-growing topics: k=50 cluster growth rate
# ============================================================
print("\nComputing cluster growth rates...")
# Compare 2016-2019 vs 2020-2024 proportions
labels = pd.read_csv(OUTPUT_DIR / "cluster_labels_k50.csv")
label_map = dict(zip(labels["cluster"], labels["label"]))

era_recent = df[df["era"] == "2020-2024"]["kmeans_50"].value_counts()
era_prior = df[df["era"] == "2016-2019"]["kmeans_50"].value_counts()
n_recent = (df["era"] == "2020-2024").sum()
n_prior = (df["era"] == "2016-2019").sum()

growth = pd.DataFrame({
    "recent_pct": (era_recent / n_recent * 100),
    "prior_pct": (era_prior / n_prior * 100),
}).fillna(0)
growth["change"] = growth["recent_pct"] - growth["prior_pct"]
growth["ratio"] = growth["recent_pct"] / growth["prior_pct"].replace(0, np.nan)
growth["label"] = growth.index.map(label_map)
growth["n_recent"] = era_recent
growth["n_prior"] = era_prior

# Filter out www/genome clusters
growth_clean = growth[~growth["label"].str.contains("www", na=False)]

# Top growers
growers = growth_clean.sort_values("change", ascending=False).head(10)
print("\n  Fastest growing topics (2020-2024 vs 2016-2019):")
print(f"  {'Cluster':<8} {'Recent%':>8} {'Prior%':>8} {'Change':>8} {'Label'}")
for idx, row in growers.iterrows():
    print(f"  {idx:<8} {row['recent_pct']:>7.1f}% {row['prior_pct']:>7.1f}% {row['change']:>+7.1f}  {row['label']}")

# Top decliners
decliners = growth_clean.sort_values("change", ascending=True).head(10)
print("\n  Fastest declining topics (2020-2024 vs 2016-2019):")
print(f"  {'Cluster':<8} {'Recent%':>8} {'Prior%':>8} {'Change':>8} {'Label'}")
for idx, row in decliners.iterrows():
    print(f"  {idx:<8} {row['recent_pct']:>7.1f}% {row['prior_pct']:>7.1f}% {row['change']:>+7.1f}  {row['label']}")

# ============================================================
# 7. Growth/decline bar chart
# ============================================================
top_movers = pd.concat([growers.head(8), decliners.head(8)])
top_movers = top_movers.sort_values("change")

fig, ax = plt.subplots(figsize=(12, 8))
colors = ["#d73027" if v < 0 else "#1a9850" for v in top_movers["change"]]
bars = ax.barh(range(len(top_movers)), top_movers["change"], color=colors)
ax.set_yticks(range(len(top_movers)))
ax.set_yticklabels([f"C{idx}: {row['label'][:35]}" for idx, row in top_movers.iterrows()], fontsize=9)
ax.set_xlabel("Change in % share (2020-2024 vs 2016-2019)")
ax.set_title("Fastest Growing and Declining Research Topics in GEO", fontsize=13)
ax.axvline(0, color="black", linewidth=0.5)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "topic_growth_decline.png", dpi=150, bbox_inches="tight")
plt.close()
print("\n  Saved topic_growth_decline.png")

# ============================================================
# 8. UMAP colored by modality
# ============================================================
print("\nPlotting UMAP by modality...")
modalities = ["RNA-seq", "Microarray", "ChIP-seq", "Methylation", "ncRNA", "Other"]
mod_colors = {"RNA-seq": "#e41a1c", "Microarray": "#377eb8", "ChIP-seq": "#4daf4a",
              "Methylation": "#984ea3", "ncRNA": "#ff7f00", "Other": "#999999",
              "Genomic variation": "#a65628"}

fig, axes = plt.subplots(2, 3, figsize=(21, 13))
for ax, mod in zip(axes.flat, modalities):
    mask = df["modality"] == mod
    ax.scatter(df["umap_1"], df["umap_2"], s=0.1, c="lightgrey", alpha=0.05)
    ax.scatter(df.loc[mask, "umap_1"], df.loc[mask, "umap_2"],
               s=0.3, c=mod_colors.get(mod, "red"), alpha=0.3)
    ax.set_title(f"{mod} (n={mask.sum():,})", fontsize=13)
    ax.set_xticks([]); ax.set_yticks([])
plt.suptitle("GEO Projects by Modality on UMAP", fontsize=16, y=1.01)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "umap_by_modality.png", dpi=200, bbox_inches="tight")
plt.close()
print("  Saved umap_by_modality.png")

# ============================================================
# 9. UMAP colored by organism
# ============================================================
print("Plotting UMAP by organism...")
org_colors = {"Homo sapiens": "#e41a1c", "Mus musculus": "#377eb8",
              "Drosophila melanogaster": "#4daf4a", "Arabidopsis thaliana": "#984ea3",
              "Rattus norvegicus": "#ff7f00"}

fig, axes = plt.subplots(1, 5, figsize=(30, 5.5))
for ax, org in zip(axes, top_orgs):
    mask = df["series_organism"] == org
    ax.scatter(df["umap_1"], df["umap_2"], s=0.1, c="lightgrey", alpha=0.05)
    ax.scatter(df.loc[mask, "umap_1"], df.loc[mask, "umap_2"],
               s=0.3, c=org_colors[org], alpha=0.3)
    short = org.split()[-1] if len(org.split()) > 1 else org
    ax.set_title(f"{org} (n={mask.sum():,})", fontsize=11)
    ax.set_xticks([]); ax.set_yticks([])
plt.suptitle("GEO Projects by Organism on UMAP", fontsize=15)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "umap_by_organism.png", dpi=200, bbox_inches="tight")
plt.close()
print("  Saved umap_by_organism.png")

# ============================================================
# 10. China vs USA UMAP by era (2x2 panel)
# ============================================================
print("Plotting USA vs China by era...")
fig, axes = plt.subplots(2, 2, figsize=(16, 14))
panels = [("USA", "2001-2015"), ("USA", "2016-2024"),
          ("China", "2001-2015"), ("China", "2016-2024")]
for ax, (country, period) in zip(axes.flat, panels):
    if "2015" in period:
        mask = (df["series_contact_country"] == country) & (df["year"] <= 2015)
    else:
        mask = (df["series_contact_country"] == country) & (df["year"] >= 2016)
    ax.scatter(df["umap_1"], df["umap_2"], s=0.1, c="lightgrey", alpha=0.05)
    color = "#1f77b4" if country == "USA" else "#d62728"
    ax.scatter(df.loc[mask, "umap_1"], df.loc[mask, "umap_2"],
               s=0.3, c=color, alpha=0.3)
    ax.set_title(f"{country} {period} (n={mask.sum():,})", fontsize=13)
    ax.set_xticks([]); ax.set_yticks([])
plt.suptitle("USA vs China: Research Focus Shift Over Time", fontsize=16, y=1.01)
plt.tight_layout()
plt.savefig(OUTPUT_DIR / "umap_usa_china_eras.png", dpi=200, bbox_inches="tight")
plt.close()
print("  Saved umap_usa_china_eras.png")

# ============================================================
# Export stats for report
# ============================================================
print("\nExporting recency stats...")
stats = {}

# Era counts
for era in era_order:
    stats[f"n_{era.replace('-','_')}"] = int((df["era"] == era).sum())

# Modality by era
for era in era_order:
    era_key = era.replace("-", "_")
    for mod in ["RNA-seq", "Microarray", "ChIP-seq"]:
        val = mod_era_pct.loc[era, mod] if mod in mod_era_pct.columns else 0
        stats[f"mod_{mod.lower().replace('-','_')}_{era_key}"] = round(float(val), 1)

# Country shifts
for era in ["2001-2010", "2020-2024"]:
    era_key = era.replace("-", "_")
    for c in ["USA", "China"]:
        val = country_era_pct.loc[era, c] if c in country_era_pct.columns else 0
        stats[f"country_{c.lower()}_{era_key}"] = round(float(val), 1)

# Top growers/decliners
stats["top_growers"] = []
for idx, row in growers.head(5).iterrows():
    stats["top_growers"].append({
        "cluster": int(idx), "label": row["label"],
        "recent_pct": round(row["recent_pct"], 2),
        "prior_pct": round(row["prior_pct"], 2),
        "change": round(row["change"], 2),
    })
stats["top_decliners"] = []
for idx, row in decliners.head(5).iterrows():
    stats["top_decliners"].append({
        "cluster": int(idx), "label": row["label"],
        "recent_pct": round(row["recent_pct"], 2),
        "prior_pct": round(row["prior_pct"], 2),
        "change": round(row["change"], 2),
    })

with open(OUTPUT_DIR / "recency_stats.json", "w") as f:
    json.dump(stats, f, indent=2)
print(f"  Saved recency_stats.json")

print("\nDone.")
