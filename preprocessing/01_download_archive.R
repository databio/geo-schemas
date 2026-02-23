# 01_download_archive.R
# Download and extract the GEO PEPhub archive

library(fs)

# Config
ARCHIVE_URL <- "https://cloud2.databio.org/pephub/geo/geo_2025_04_28.tar"
DATA_DIR <- here::here("data")
ARCHIVE_PATH <- path(DATA_DIR, "geo_archive.tar")
EXTRACT_DIR <- path(DATA_DIR, "geo")

#' Download the GEO namespace archive from PEPhub
#' @param force Re-download even if file exists
download_geo_archive <- function(force = FALSE) {
  dir_create(DATA_DIR)

  if (file_exists(ARCHIVE_PATH) && !force) {
    message("Archive already exists: ", ARCHIVE_PATH)
    message("Size: ", format(file_size(ARCHIVE_PATH), units = "auto"))
    return(invisible(ARCHIVE_PATH))
  }

  message("Downloading GEO archive (~1.4 GB)...")
  message("URL: ", ARCHIVE_URL)

  # Use curl for progress bar
  download.file(
    ARCHIVE_URL,
    destfile = ARCHIVE_PATH,
    mode = "wb",
    method = "curl",
    extra = "-#"
  )

  message("Download complete: ", ARCHIVE_PATH)
  invisible(ARCHIVE_PATH)
}

#' Extract the archive
#' @param force Re-extract even if directory exists
extract_geo_archive <- function(force = FALSE) {
  if (!file_exists(ARCHIVE_PATH)) {
    stop("Archive not found. Run download_geo_archive() first.")
  }

  if (dir_exists(EXTRACT_DIR) && !force) {
    n_projects <- length(dir_ls(EXTRACT_DIR, type = "directory"))
    message("Archive already extracted: ", EXTRACT_DIR)
    message("Projects found: ", format(n_projects, big.mark = ","))
    return(invisible(EXTRACT_DIR))
  }

  message("Extracting archive...")
  dir_create(EXTRACT_DIR)

  untar(ARCHIVE_PATH, exdir = DATA_DIR)

  n_projects <- length(dir_ls(EXTRACT_DIR, type = "directory"))
  message("Extraction complete. Projects: ", format(n_projects, big.mark = ","))
  invisible(EXTRACT_DIR)
}

# Run if executed directly
if (sys.nframe() == 0) {
  download_geo_archive()
  extract_geo_archive()
}
