library(fs)
library(yaml)
library(data.table)
library(arrow)

DATA_DIR <- here::here("data")
ARCHIVE_PATH <- dir_ls(DATA_DIR, glob = "*.tar")[1]  # Find the tar file
PEPS_DIR <- path(DATA_DIR, "peps")
OUTPUT_PATH <- path(DATA_DIR, "geo_metadata.parquet")

df <- read_parquet(OUTPUT_PATH)

