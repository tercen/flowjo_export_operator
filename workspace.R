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
if (length(rows) <= 2) {
  stop("There should be at least three rows: filename, rowId and a result")
}

output_folder    <- ifelse(is.null(ctx$op.value('output_folder')), "exporting data", as.character(ctx$op.value('output_folder')))
cluster_na_value <- ifelse(is.null(ctx$op.value('cluster_na_value')), 0, as.numeric(ctx$op.value('cluster_na_value')))
other_na_value   <- ifelse(is.null(ctx$op.value('other_na_value')), 0, as.numeric(ctx$op.value('other_na_value')))

# create output folder
project   <- ctx$client$projectService$get(ctx$schema$projectId)
folder    <- NULL
if (output_folder != "") {
  folder  <- ctx$client$folderService$getOrCreate(project$id, output_folder)
}

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

get_numeric_value <- function(value, col_name) {
  result <- NULL
  cl <- class(value)
  if (cl == "character") {
    result <- tryCatch(expr = as.numeric(regmatches(value, gregexpr("[[:digit:]]+", value))),
                       error = function(e) {
                         return(NULL)
              })
    if (is.null(result)) {
      result <- value
      non_empty_result      <- result[result != ""]
      result[result != ""]  <- as.numeric(factor(non_empty_result))
      result[result == ""]  <- NA
      result[is.na(result)] <- cluster_na_value
      df_cluster_mapping    <- data.frame(cluster = unique(factor(non_empty_result)), cluster_number = unique(as.numeric(factor(non_empty_result))))
    } else {
      result[is.na(result)] <- cluster_na_value
      df_cluster_mapping    <- data.frame(cluster = c(NA, unique(value)[unique(value) != ""]), cluster_number = unique(result)[complete.cases(unique(result))])
    }
    upload_data(df_cluster_mapping %>% arrange(cluster_number), folder, paste0(col_name, "_cluster_mapping"), project, ctx$client)
  } else if (cl == "numeric") {
    value[is.nan(value)] <- other_na_value
    result <- value
  }
  result
}

df <- rows %>%
  select(-rowId) %>%
  mutate(across(!filename, ~ get_numeric_value(., cur_column()))) %>%
  mutate(across(where(is.character), trimws))

is_whole_number <- function(x) all(floor(x) == x)
type_result     <- sapply(df, function(x) if (class(x) == "numeric") { is_whole_number(x) })
new_colnames    <- lapply(colnames(df), FUN = function(x) {
  result <- x
  type   <- type_result[[x]]
  if (!is.null(type)) {
    if (isTRUE(type)) {
      result <- paste0(result, ".int")
    } else {
      result <- paste0(result, ".double")
    }
  }
  return(result)
})
df <- df %>% `colnames<-`(new_colnames)

# Save a table for each file
filenames <- unique(df$filename)
lapply(filenames, FUN = function(filename) {
  df_file  <- df %>% filter(filename == filename) %>% select(-filename)
  last_col <- rev(colnames(df_file))[1]
  out_name <- gsub('[[:digit:]]+', '', unlist(strsplit(last_col, "\\."))[[2]])
  filename <- paste0(filename, "_", out_name)
  upload_data(df_file, folder, filename, project, ctx$client)
})

df %>% 
  mutate(.ri = seq(1, nrow(.))) %>%
  ctx$addNamespace() %>%
  ctx$save()
