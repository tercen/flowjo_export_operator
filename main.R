library(tercen)
library(dplyr, warn.conflicts = FALSE)

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

upload_data <- function(df, file_name, project, client) {
  fileDoc = FileDocument$new()
  fileDoc$name = paste0("Export-", file_name)
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
}

df <- ctx %>%
  select(.y, .ci, .ri) %>%
  group_by(.ri, .ci) %>%
  summarise(.y = mean(.y)) %>%
  ungroup() %>%
  cbind(ctx$rselect()) %>%
  left_join(clusters) %>%
  mutate(cluster = ifelse(.[[6]] == "", -1, as.numeric(gsub("c", "", .[[6]])))) %>%
  select(filename, rowId, cluster)

# Save a table for each file
filenames <- unique(df$filename)
project   <- ctx$client$projectService$get(ctx$schema$projectId)
lapply(filenames, FUN = function(filename) {
  df <- df %>% filter(filename == filename) %>% select(2,3)
  upload_data(df, filename, project, ctx$client)
})
