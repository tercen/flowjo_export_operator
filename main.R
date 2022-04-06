library(tercen)
library(dplyr, warn.conflicts = FALSE)

ctx = tercenCtx()

ctx %>%
  select(.y, .ci, .ri) %>%
  group_by(.ri, .ci) %>%
  summarise(.y = mean(.y)) %>%
  ungroup() %>%
  cbind(ctx$rselect()) %>%
  mutate(cluster = .ci+1) %>%
  select(.ri, cluster) %>%
  ctx$addNamespace() %>%
  ctx$save()