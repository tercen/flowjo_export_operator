library(tercen)
library(dplyr, warn.conflicts = FALSE)

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
  mutate(cluster = as.numeric(gsub("c", "", .[[5]]))) %>%
  select(.ri, cluster) %>%
  ctx$addNamespace() %>%
  ctx$save()
