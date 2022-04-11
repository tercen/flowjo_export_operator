library(tercen)
library(dplyr, warn.conflicts = FALSE)

# Set appropriate options
#options("tercen.serviceUri"="http://tercen:5400/api/v1/")
#options("tercen.workflowId"= "4133245f38c1411c543ef25ea3020c41")
#options("tercen.stepId"= "2b6d9fbf-25e4-4302-94eb-b9562a066aa5")
#options("tercen.username"= "admin")
#options("tercen.password"= "admin")

ctx = tercenCtx()

rows <- ctx$rselect()
if (length(rows) != 2) {
  stop("There should be two rows: filename and rowId")
}

clusters <- ctx$cselect()
if (length(clusters) != 1) {
  stop("There should be a single column for the clusters")
}

clusters <- cbind(clusters, .ci = seq(0, nrow(clusters) - 1))

output_folder <- ifelse(is.null(ctx$op.value('output_folder')), "exporting data", as.character(ctx$op.value('output_folder')))

upload_data <- function(df, folder, file_name, project, client) {
  fileDoc = FileDocument$new()
  fileDoc$name = paste0("Export-", file_name)
  fileDoc$folderId = folder$id
  fileDoc$projectId = project$id
  fileDoc$acl$owner = project$acl$owner
  fileDoc$metadata = CSVFileMetadata$new()
  fileDoc$metadata$contentType = 'text/csv'
  fileDoc$metadata$separator = ','
  fileDoc$metadata$quote = '"'
  fileDoc$metadata$contentEncoding = 'iso-8859-1'
  
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
  schema = client$tableSchemaService$get(task$schemaId)
  schema$folderId = folder$id
  client$tableSchemaService$update(schema)
}

df <- ctx %>%
  select(.y, .ci, .ri) %>%
  group_by(.ri, .ci) %>%
  summarise(.y = mean(.y)) %>%
  ungroup() %>%
  cbind(ctx$rselect()) %>%
  left_join(clusters) %>%
  mutate(cluster = ifelse(.[[6]] == "", -1, as.numeric(gsub("c", "", .[[6]])))) %>%
  select(.ri, filename, rowId, cluster)

# Save a table for each file
project   <- ctx$client$projectService$get(ctx$schema$projectId)
folder    <- ctx$client$folderService$getOrCreate(project$id, output_folder)
filenames <- unique(df$filename)
lapply(filenames, FUN = function(filename) {
  df_file <- df %>% filter(filename == filename) %>% select(rowId, cluster)
  upload_data(df_file, folder, filename, project, ctx$client)
})

df %>%
  ctx$addNamespace() %>%
  ctx$save()
