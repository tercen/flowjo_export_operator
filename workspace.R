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
if (length(rows) != 1) {
  stop("There should be a single row containing the rowId")
}

clusters <- ctx$cselect()
if (length(clusters) != 1) {
  stop("There should be a single column for the clusters")
}

clusters <- cbind(clusters, .ci = seq(0, nrow(clusters) - 1))

ctx %>%
  select(.y, .ci, .ri) %>%
  group_by(.ri, .ci) %>%
  summarise(.y = mean(.y)) %>%
  ungroup() %>%
  cbind(ctx$rselect()) %>%
  left_join(clusters) %>%
  mutate(cluster = as.integer(gsub("c", "", .[[5]]))) %>%
  select(.ri, cluster) %>%
  ctx$addNamespace() %>%
  ctx$save()
