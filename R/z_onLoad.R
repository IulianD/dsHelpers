.onLoad <- function(...){
  options(hidden.fields = unique(c(getOption('hidden.fields'),'SUBJID', 'USUBJID')))
  options(join.pivot.col = unique(c(getOption('join.pivot.col'),'SUBJID')))
}
