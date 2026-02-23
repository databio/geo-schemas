# 02_parse_peps_api.R
# Alternative: Fetch PEP metadata via PEPhub API (slower but no download needed)
# Use this for testing or if archive is unavailable

library(httr2)
library(data.table)
library(arrow)
library(furrr)
library(progressr)
library(fs)

DATA_DIR <- here::here("data")
OUTPUT_PATH <- path(DATA_DIR, "geo_metadata.parquet")
API_BASE <- "https://pephub-api.databio.org/api/v1"

#' List all projects in the geo namespace
#' @param limit Max projects to fetch (NULL for all)
#' @param batch_size Projects per API request
list_geo_projects <- function(limit = NULL, batch_size = 100) {
  projects <- list()
  offset <- 0

  repeat {
    resp <- request(API_BASE) |>
      req_url_path_append("namespaces", "geo", "projects") |>
      req_url_query(limit = batch_size, offset = offset) |>
      req_perform() |>
      resp_body_json()

    projects <- c(projects, resp$results)
    offset <- offset + batch_size

    message("\rFetched ", length(projects), " / ", resp$count, " projects", appendLF = FALSE)

    if (length(resp$results) < batch_size) break
    if (!is.null(limit) && length(projects) >= limit) break
  }
  message()

  # Extract just the project names
  sapply(projects, `[[`, "name")
}

#' Fetch full project data including samples
#' @param project_name GSE ID
fetch_project <- function(project_name) {
  tryCatch({
    resp <- request(API_BASE) |>
      req_url_path_append("projects", "geo", project_name) |>
      req_url_query(tag = "default") |>
      req_perform() |>
      resp_body_json()

    config <- resp$config
    samples <- resp$sample_list

    # Extract column names from first sample
    sample_columns <- if (length(samples) > 0) names(samples[[1]]) else character(0)

    exp_meta <- config$experiment_metadata %||% list()

    data.table(
      gse_id = project_name,
      n_samples = length(samples),
      n_columns = length(sample_columns),
      column_names = list(sample_columns),
      series_title = exp_meta$series_title %||% NA_character_,
      series_summary = exp_meta$series_summary %||% NA_character_,
      series_type = exp_meta$series_type %||% NA_character_,
      series_organism = exp_meta$series_sample_organism %||% NA_character_,
      series_contributor = exp_meta$series_contributor %||% NA_character_,
      series_contact_country = exp_meta$series_contact_country %||% NA_character_,
      series_contact_institute = exp_meta$series_contact_institute %||% NA_character_,
      series_submission_date = exp_meta$series_submission_date %||% NA_character_,
      series_platform_id = exp_meta$series_platform_id %||% NA_character_,
      description = config$description %||% NA_character_,
      pep_version = config$pep_version %||% NA_character_
    )
  }, error = function(e) {
    warning("Failed to fetch ", project_name, ": ", e$message)
    NULL
  })
}

#' Fetch all projects via API
#' @param n_projects Number of projects to fetch (NULL for all)
#' @param n_workers Number of parallel workers
#' @param delay_ms Delay between requests to avoid rate limiting
fetch_all_projects <- function(n_projects = 100, n_workers = 4, delay_ms = 100) {
  message("Listing geo projects...")
  project_names <- list_geo_projects(limit = n_projects)

  message("Fetching ", length(project_names), " projects...")

  plan(multisession, workers = n_workers)
  on.exit(plan(sequential), add = TRUE)

  handlers(global = TRUE)
  handlers("txtprogressbar")

  with_progress({
    p <- progressor(steps = length(project_names))
    results <- future_map(project_names, function(name) {
      Sys.sleep(delay_ms / 1000)  # Rate limiting
      p()
      fetch_project(name)
    }, .options = furrr_options(seed = TRUE))
  })

  dt <- rbindlist(results, fill = TRUE)
  message("Successfully fetched ", nrow(dt), " projects")
  dt
}

#' Save to parquet (same as archive version)
save_metadata <- function(dt, output_path = OUTPUT_PATH) {
  dir_create(path_dir(output_path))

  dt_out <- copy(dt)
  dt_out[, column_names_json := sapply(column_names, function(x) {
    if (length(x) == 0) "[]" else jsonlite::toJSON(x)
  })]
  dt_out[, column_names := NULL]

  write_parquet(dt_out, output_path)
  message("Saved to: ", output_path)
  invisible(output_path)
}

# Run if executed directly
if (sys.nframe() == 0) {
  # Fetch small sample for testing
  dt <- fetch_all_projects(n_projects = 50, n_workers = 2)
  print(dt)

  # Uncomment to save:
  # save_metadata(dt)
}
