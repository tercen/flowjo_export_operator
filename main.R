library(tercen)
library(tercenApi)
library(data.table)
library(dplyr, warn.conflicts = FALSE)
library(tidyr)

upload_data <- function(df, folder, file_name, project, client) {
  fileDoc = FileDocument$new()
  fileDoc$name = paste0("Export-", file_name)
  fileDoc$projectId = project$id
  fileDoc$acl$owner = project$acl$owner
  fileDoc$metadata = CSVFileMetadata$new()
  fileDoc$metadata$contentType = 'text/csv'
  fileDoc$metadata$separator = ','
  fileDoc$metadata$quote = '"'
  fileDoc$metadata$contentEncoding = 'iso-8859-1'
  if (!is.null(folder)) {
    fileDoc$folderId = folder$id
  }
  
  tmp_file <- tempfile()
  data.table::fwrite(df, tmp_file, row.names = FALSE, na = "NA")
  
  file <- file(tmp_file, 'rb')
  bytes = readBin(file, raw(), n=file.info(tmp_file)$size)
  fileDoc = client$fileService$upload(fileDoc, bytes)
  close(file)
  
  task = CSVTask$new()
  task$state = InitState$new()
  task$fileDocumentId = fileDoc$id
  task$owner = project$acl$owner
  task$projectId = project$id
  task$params$separator = ','
  task$params$encoding = 'iso-8859-1'
  task$params$quote = '"'
  task = client$taskService$create(task)
  client$taskService$runTask(task$id)
  task = client$taskService$waitDone(task$id)
  
  if (inherits(task$state, 'FailedState')){
    stop(task$state$reason)
  }
  
  # move file to folder
  if (!is.null(folder)) {
    schema = client$tableSchemaService$get(task$schemaId)
    schema$folderId = folder$id
    client$tableSchemaService$update(schema)
  }
}

ctx = tercenCtx()

rows <- ctx$rselect()
if (length(rows) <= 2) {
  stop("There should be at least three rows: filename, rowId and a result")
}
if(!"filename" %in% colnames(rows)) stop("filename is required.")
if(!"rowId" %in% colnames(rows))    stop("rowId is required.")

output_folder      <- ctx$op.value('output_folder', as.character, "exporting data")
character_na_value <- ctx$op.value('character_na_value', as.numeric, 0)
integer_na_value   <- ctx$op.value('integer_na_value', as.numeric, 0)
double_na_value    <- ctx$op.value('double_na_value', as.numeric, 0)

# create output folder
project   <- ctx$client$projectService$get(ctx$schema$projectId)
folder    <- NULL
if (output_folder != "") {
  folder  <- ctx$client$folderService$getOrCreate(project$id, output_folder)
}

df <- rows %>%
  select(-rowId) %>%
  mutate(across(where(is.character), trimws)) %>%
  mutate_if(is.integer,   ~replace_na(., integer_na_value))   %>%
  mutate_if(is.double,    ~replace_na(., double_na_value))    %>%
  mutate_if(is.character, ~replace_na(., character_na_value))

# Save a table for each file
do.upload <- function(df_tmp, folder, project, ctx) {
  filename <- df_tmp$filename[1]
  df_tmp <- select(df_tmp, -filename)
  col_names <- colnames(df_tmp)
  out_name <- paste(unique(unlist(lapply(
    col_names,
    FUN = function(x) { substr(unlist(strsplit(x, "\\."))[2], 1, 11) }
  ))), collapse = "_")
  filename  <- paste0(filename, "_", out_name)
  upload_data(df_tmp, folder, filename, project, ctx$client)
  return(df_tmp)
}

df %>% 
  group_by(filename) %>% 
  do(do.upload(., folder, project, ctx))

df %>% 
  mutate(.ri = seq(0, nrow(.) - 1)) %>%
  ctx$addNamespace() %>%
  ctx$save()
