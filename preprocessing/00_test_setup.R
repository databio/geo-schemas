# 00_test_setup.R
# Quick test to verify setup and fetch a small sample via API

# Check required packages (minimal for test)
test_pkgs <- c("httr2", "data.table")
missing_test <- test_pkgs[!sapply(test_pkgs, requireNamespace, quietly = TRUE)]
if (length(missing_test) > 0) {
  stop("Missing packages for test: ", paste(missing_test, collapse = ", "),
       "\nInstall with: install.packages(c('", paste(missing_test, collapse = "', '"), "'))")
}

# Check full pipeline packages
full_pkgs <- c("fs", "yaml", "data.table", "arrow", "httr2", "furrr",
               "progressr", "here", "jsonlite")
missing_full <- full_pkgs[!sapply(full_pkgs, requireNamespace, quietly = TRUE)]
if (length(missing_full) > 0) {
  message("Note: Missing packages for full pipeline: ", paste(missing_full, collapse = ", "))
  message("Install with: install.packages(c('", paste(missing_full, collapse = "', '"), "'))")
} else {
  message("All required packages installed!")
}

# Quick API test - fetch 5 projects
library(httr2)
library(data.table)

API_BASE <- "https://pephub-api.databio.org/api/v1"

message("\nTesting PEPhub API...")

# Get project list
resp <- request(API_BASE) |>
  req_url_path_append("namespaces", "geo", "projects") |>
  req_url_query(limit = 5) |>
  req_perform() |>
  resp_body_json()

message("Total GEO projects: ", format(resp$count, big.mark = ","))
message("Sample projects: ", paste(sapply(resp$results, `[[`, "name"), collapse = ", "))

# Fetch one project's full data
test_project <- resp$results[[1]]$name
message("\nFetching full data for: ", test_project)

proj_resp <- request(API_BASE) |>
  req_url_path_append("projects", "geo", test_project) |>
  req_url_query(tag = "default") |>
  req_perform() |>
  resp_body_json()

config <- proj_resp$config
samples <- proj_resp$sample_list
exp_meta <- config$experiment_metadata

message("  Samples: ", length(samples))
message("  Columns: ", length(names(samples[[1]])))
message("  Title: ", substr(exp_meta$series_title %||% "N/A", 1, 60), "...")

# Show sample column names
col_names <- names(samples[[1]])
message("\nSample columns (first 10):")
cat("  ", paste(head(col_names, 10), collapse = "\n  "), "\n")

message("\n--- Setup test passed! ---")
message("Next steps:")
message("  1. For full dataset: source('preprocessing/01_download_archive.R')")
message("  2. For quick test:   source('preprocessing/02_parse_peps_api.R')")
