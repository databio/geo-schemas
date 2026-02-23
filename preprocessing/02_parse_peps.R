# 02_parse_peps.R
# Parse PEP files from the GEO archive (tar of zips)

library(fs)
library(yaml)
library(data.table)
library(arrow)

DATA_DIR <- here::here("data")
ARCHIVE_PATH <- dir_ls(DATA_DIR, glob = "*.tar")[1]  # Find the tar file
PEPS_DIR <- path(DATA_DIR, "peps")
OUTPUT_PATH <- path(DATA_DIR, "geo_metadata.parquet")

#' Extract the tar archive (gets zip files)
extract_tar <- function(force = FALSE) {
  if (dir_exists(PEPS_DIR) && !force) {
    n_zips <- length(dir_ls(PEPS_DIR, glob = "*.zip", recurse = TRUE))
    message("Already extracted. Zip files: ", format(n_zips, big.mark = ","))
    return(invisible(PEPS_DIR))
  }
  message("Extracting tar archive...")
  untar(ARCHIVE_PATH, exdir = DATA_DIR)
  message("Done.")
  invisible(PEPS_DIR)
}

#' Parse a single PEP zip file
#' @param zip_path Path to the project zip
#' @return data.table with one row, or NULL on failure
parse_pep_zip <- function(zip_path) {
  gse_id <- path_ext_remove(path_file(zip_path))

  tryCatch({
    files <- unzip(zip_path, list = TRUE)$Name
    yaml_file <- grep("_config\\.yaml$|\\.yaml$", files, value = TRUE)[1]
    csv_file <- grep("\\.csv$", files, value = TRUE)[1]

    if (is.na(yaml_file)) return(NULL)

    # Read YAML config from zip
    config <- read_yaml(text = readLines(
      unz(zip_path, yaml_file), warn = FALSE
    ))

    # Read CSV header from zip
    sample_columns <- character(0)
    n_samples <- 0L
    if (!is.na(csv_file)) {
      con <- unz(zip_path, csv_file)
      lines <- readLines(con, warn = FALSE)
      close(con)
      if (length(lines) > 0) {
        sample_columns <- names(fread(text = lines[1]))
        n_samples <- max(0L, length(lines) - 1L)
      }
    }

    exp_meta <- config$experiment_metadata %||% list()

    data.table(
      gse_id = gse_id,
      n_samples = n_samples,
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
      series_overall_design = exp_meta$series_overall_design %||% NA_character_,
      description = config$description %||% NA_character_,
      pep_version = config$pep_version %||% NA_character_
    )
  }, error = function(e) {
    warning("Failed to parse ", gse_id, ": ", e$message)
    NULL
  })
}

#' Parse all PEP zips
#' @param sample_n Parse only this many (for testing)
#' @param batch_size Print progress every N projects
parse_all_peps <- function(sample_n = NULL, batch_size = 5000) {
  extract_tar()

  zip_files <- dir_ls(PEPS_DIR, glob = "*.zip", recurse = TRUE)
  message("Found ", format(length(zip_files), big.mark = ","), " zip files")

  if (!is.null(sample_n)) {
    zip_files <- sample(zip_files, min(sample_n, length(zip_files)))
    message("Sampling ", length(zip_files), " for testing")
  }

  n <- length(zip_files)
  results <- vector("list", n)

  for (i in seq_len(n)) {
    results[[i]] <- parse_pep_zip(zip_files[i])
    if (i %% batch_size == 0) {
      message("  ", format(i, big.mark = ","), " / ", format(n, big.mark = ","))
    }
  }

  dt <- rbindlist(results, fill = TRUE)
  message("Parsed ", format(nrow(dt), big.mark = ","), " projects")
  dt
}

#' Save parsed metadata to parquet
save_metadata <- function(dt, output_path = OUTPUT_PATH) {
  dir_create(path_dir(output_path))

  dt_out <- copy(dt)
  dt_out[, column_names_json := vapply(column_names, function(x) {
    if (length(x) == 0) "[]" else jsonlite::toJSON(x)
  }, character(1))]
  dt_out[, column_names := NULL]

  write_parquet(dt_out, output_path)
  message("Saved to: ", output_path)
  message("Size: ", format(file_size(output_path), units = "auto"))
  invisible(output_path)
}

# Run if executed directly
if (sys.nframe() == 0) {
  dt <- parse_all_peps()
  save_metadata(dt)
}
