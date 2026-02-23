#!/usr/bin/env Rscript
# Step 1: Preliminary categorical analysis of GEO series metadata
# Issue #55 — GEO Series-Level Semantic Meta Analysis

library(arrow)
library(data.table)
library(ggplot2)
library(lubridate)

outdir <- "../output"
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

# Load data
cat("Loading geo_metadata.parquet...\n")
dt <- as.data.table(read_parquet("../../data/geo_metadata.parquet"))
cat(sprintf("Loaded %d rows, %d columns\n", nrow(dt), ncol(dt)))

# --- Parse submission dates ---
dt[, submission_date := parse_date_time(series_submission_date, orders = "b d Y")]
dt[, submission_year := year(submission_date)]

# ============================================================
# 1. Series type distribution
# ============================================================
cat("\n--- Series Type Distribution ---\n")
type_counts <- dt[, .N, by = series_type][order(-N)]
print(head(type_counts, 20))
fwrite(type_counts, file.path(outdir, "series_type_counts.csv"))

p1 <- ggplot(type_counts[1:15], aes(x = reorder(series_type, N), y = N)) +
  geom_col(fill = "steelblue") +
  coord_flip() +
  labs(title = "Top 15 GEO Series Types", x = NULL, y = "Number of Projects") +
  theme_minimal(base_size = 12)
ggsave(file.path(outdir, "series_type_top15.png"), p1, width = 10, height = 6, dpi = 150)

# ============================================================
# 2. Projects per year
# ============================================================
cat("\n--- Projects per Year ---\n")
year_counts <- dt[!is.na(submission_year), .N, by = submission_year][order(submission_year)]
print(year_counts)
fwrite(year_counts, file.path(outdir, "projects_per_year.csv"))

p2 <- ggplot(year_counts, aes(x = submission_year, y = N)) +
  geom_col(fill = "steelblue") +
  labs(title = "GEO Projects per Year", x = "Submission Year", y = "Number of Projects") +
  theme_minimal(base_size = 12)
ggsave(file.path(outdir, "projects_per_year.png"), p2, width = 10, height = 5, dpi = 150)

# ============================================================
# 3. Top organisms
# ============================================================
cat("\n--- Top Organisms ---\n")
organism_counts <- dt[, .N, by = series_organism][order(-N)]
print(head(organism_counts, 20))
fwrite(organism_counts, file.path(outdir, "organism_counts.csv"))

p3 <- ggplot(organism_counts[1:15], aes(x = reorder(series_organism, N), y = N)) +
  geom_col(fill = "darkgreen") +
  coord_flip() +
  labs(title = "Top 15 GEO Organisms", x = NULL, y = "Number of Projects") +
  theme_minimal(base_size = 12)
ggsave(file.path(outdir, "organism_top15.png"), p3, width = 10, height = 6, dpi = 150)

# ============================================================
# 4. Top countries
# ============================================================
cat("\n--- Top Countries ---\n")
country_counts <- dt[nchar(series_contact_country) > 0, .N, by = series_contact_country][order(-N)]
print(head(country_counts, 20))
fwrite(country_counts, file.path(outdir, "country_counts.csv"))

p4 <- ggplot(country_counts[1:20], aes(x = reorder(series_contact_country, N), y = N)) +
  geom_col(fill = "darkorange") +
  coord_flip() +
  labs(title = "Top 20 GEO Countries", x = NULL, y = "Number of Projects") +
  theme_minimal(base_size = 12)
ggsave(file.path(outdir, "country_top20.png"), p4, width = 10, height = 6, dpi = 150)

# ============================================================
# 5. Top institutions
# ============================================================
cat("\n--- Top Institutions ---\n")
inst_counts <- dt[nchar(series_contact_institute) > 0, .N, by = series_contact_institute][order(-N)]
print(head(inst_counts, 20))
fwrite(inst_counts, file.path(outdir, "institution_counts.csv"))

p5 <- ggplot(inst_counts[1:20], aes(x = reorder(series_contact_institute, N), y = N)) +
  geom_col(fill = "purple4") +
  coord_flip() +
  labs(title = "Top 20 GEO Institutions", x = NULL, y = "Number of Projects") +
  theme_minimal(base_size = 12)
ggsave(file.path(outdir, "institution_top20.png"), p5, width = 10, height = 7, dpi = 150)

# ============================================================
# 6. Most prolific contributors
# ============================================================
cat("\n--- Most Prolific Contributors ---\n")
contribs <- dt[nchar(series_contributor) > 0, .(series_contributor)]
contribs_expanded <- contribs[, .(contributor = trimws(unlist(strsplit(series_contributor, "\\+")))),
                              by = seq_len(nrow(contribs))]
contribs_expanded[, contributor := gsub("\\s+", " ", contributor)]
contribs_expanded <- contribs_expanded[nchar(contributor) > 0 & contributor != ",,"]
contrib_counts <- contribs_expanded[, .N, by = contributor][order(-N)]
print(head(contrib_counts, 20))
fwrite(contrib_counts[1:min(500, nrow(contrib_counts))], file.path(outdir, "contributor_top500.csv"))

# ============================================================
# Summary stats
# ============================================================
cat("\n--- Summary ---\n")
cat(sprintf("Total projects: %d\n", nrow(dt)))
cat(sprintf("Date range: %s to %s\n",
            min(dt$submission_date, na.rm = TRUE),
            max(dt$submission_date, na.rm = TRUE)))
cat(sprintf("Unique series types: %d\n", uniqueN(dt$series_type)))
cat(sprintf("Unique organisms: %d\n", uniqueN(dt$series_organism)))
cat(sprintf("Unique countries: %d\n", uniqueN(dt$series_contact_country)))
cat(sprintf("Unique institutions: %d\n", uniqueN(dt$series_contact_institute)))

cat("\nStep 1 complete. Output in:", outdir, "\n")
