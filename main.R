library(tercen)
library(dplyr, warn.conflicts = FALSE)

ctx = tercenCtx()

rows <- ctx$rselect()
if (length(rows) <= 2) {
  stop("There should be at least three rows: filename, rowId and a result")
}

output_folder <- ifelse(is.null(ctx$op.value('output_folder')), "exporting data", as.character(ctx$op.value('output_folder')))

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
  write.csv(df, tmp_file, row.names = FALSE)
  
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

get_numeric_value <- function(value) {
  result <- NULL
  cl <- class(value)
  if (cl == "character") {
    result <- as.numeric(regmatches(value, gregexpr("[[:digit:]]+", value)))
    result[is.na(result)] <- -1
  } else if (cl == "numeric") {
    result <- value
  }
  result
}

df <- rows %>%
  select(-rowId) %>%
  mutate_at(vars(-("filename")), get_numeric_value)

# Save a table for each file
project   <- ctx$client$projectService$get(ctx$schema$projectId)
folder    <- NULL
if (output_folder != "") {
  folder  <- ctx$client$folderService$getOrCreate(project$id, output_folder)
}
filenames <- unique(df$filename)
lapply(filenames, FUN = function(filename) {
  df_file <- df %>% filter(filename == filename) %>% select(-filename)
  upload_data(df_file, folder, filename, project, ctx$client)
})

df %>% 
  mutate(.ri = seq(1, nrow(.))) %>%
  ctx$addNamespace() %>%
  ctx$save()
