library(tercen)
library(tercenApi)
library(data.table)
library(dplyr, warn.conflicts = FALSE)
library(tidyr)

source("./utils.R")

ctx = tercenCtx()

rows <- ctx$rselect()
if (length(rows) <= 2) {
  stop("There should be at least three rows: filename, rowId and a result")
}
if(!any(grepl("filename", colnames(rows)))) stop("filename is required.")
if(!any(grepl("rowId", colnames(rows))))    stop("rowId is required.")

output_folder      <- ctx$op.value('output_folder', as.character, "Exported data")
character_na_value <- ctx$op.value('character_na_value', as.numeric, 0)
integer_na_value   <- ctx$op.value('integer_na_value', as.numeric, 0)
double_na_value    <- ctx$op.value('double_na_value', as.numeric, 0)

# create output folder
project   <- ctx$client$projectService$get(ctx$schema$projectId)
folder    <- NULL
if (output_folder != "") {
  folder  <- ctx$client$folderService$getOrCreate(project$id, output_folder)
}

do.upload <- function(df_tmp, folder, project, ctx) {
  filename <- df_tmp$filename[1]
  df_tmp <- select(df_tmp, -filename)
  col_names <- colnames(df_tmp)
  out_name <- paste(unique(unlist(lapply(
    col_names,
    FUN = function(x) { substr(unlist(strsplit(x, "\\."))[2], 1, 11) }
  ))), collapse = "_")
  filename  <- paste0(filename, "_", out_name)
  upload_df(df_tmp, ctx, project, filename, folder)
  return(df_tmp)
}

rows %>%
  select(-matches("rowId")) %>%
  mutate(across(where(is.character), trimws)) %>%
  mutate_if(is.integer,   ~replace_na(., as.integer(integer_na_value)))   %>%
  mutate_if(is.double,    ~replace_na(., double_na_value))    %>%
  mutate_if(is.character, ~replace_na(., character_na_value)) %>% 
  group_by(filename) %>% 
  do(do.upload(., folder, project, ctx)) %>% 
  ungroup() %>%
  mutate(.ri = seq(0, nrow(.) - 1)) %>%
  ctx$addNamespace() %>%
  ctx$save()
