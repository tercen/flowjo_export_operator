library(tercen)
library(tercenApi)
library(data.table)
library(dplyr, warn.conflicts = FALSE)

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

get_numeric_value <- function(value, col_name) {
  result <- NULL
  cl <- class(value)
  if (cl == "character") {
    value_non_empty <- value[value != ""]
    num <- suppressWarnings(as.numeric(value_non_empty))
    isnum <- !any(is.na(num))
    if(!isnum) result <- NULL
    else result <- as.numeric(value)
    
    if (is.null(result)) {
      result <- value
      non_empty_result      <- result[result != ""]
      result[result != ""]  <- as.numeric(factor(non_empty_result))
      result[result == ""]  <- character_na_value
      result                <- as.numeric(result)
      df_cluster_mapping    <- data.frame(cluster = unique(factor(non_empty_result)), cluster_number = unique(as.numeric(factor(non_empty_result))))
    } else {
      result[is.na(result)] <- character_na_value
      cluster_vals <- unique(value)
      if (any(unique(value) == "")) {
        cluster_vals <- c(NA, unique(value)[unique(value) != ""])
      }
      df_cluster_mapping    <- data.frame(cluster = cluster_vals, cluster_number = unique(result)[complete.cases(unique(result))])
    }
    upload_data(df_cluster_mapping %>% arrange(cluster_number), folder, paste0(col_name, "_cluster_mapping"), project, ctx$client)
  } else if (cl == "numeric") {
    value[is.nan(value)] <- double_na_value
    result <- value
  } else if (cl == "integer") {
    value[is.na(value)] <- integer_na_value
    result <- value
  }
  result
}

ctx = tercenCtx()

rows <- ctx$rselect()
if (length(rows) <= 2) {
  stop("There should be at least three rows: filename, rowId and a result")
}

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
