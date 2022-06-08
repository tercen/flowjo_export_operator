upload_df <- function(df, ctx, project, filename, folder) {
  tbl = tercen::dataframe.as.table(df)
  bytes = memCompress(teRcenHttp::to_tson(tbl$toTson()),
                      type = 'gzip')
  
  fileDoc = FileDocument$new()
  fileDoc$name = paste0("Export-", filename)
  fileDoc$projectId = project$id
  fileDoc$acl$owner = project$acl$owner
  fileDoc$metadata$contentEncoding = 'gzip'
  
  if (!is.null(folder)) {
    fileDoc$folderId = folder$id
  }
  
  fileDoc = ctx$client$fileService$upload(fileDoc, bytes)
  
  task = CSVTask$new()
  task$state = InitState$new()
  task$fileDocumentId = fileDoc$id
  task$owner = project$acl$owner
  task$projectId = project$id
  
  task = ctx$client$taskService$create(task)
  ctx$client$taskService$runTask(task$id)
  task = ctx$client$taskService$waitDone(task$id)
  if (inherits(task$state, 'FailedState')){
    stop(task$state$reason)
  }
  
  if (!is.null(folder)) {
    schema = ctx$client$tableSchemaService$get(task$schemaId)
    schema$folderId = folder$id
    ctx$client$tableSchemaService$update(schema)
  }
  
  return(NULL)
}

