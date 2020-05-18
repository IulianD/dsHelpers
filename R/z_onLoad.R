.onLoad <- function(...){
  options(hidden.fields = unique(c(getOption('hidden.fields'),'SUBJID', 'USUBJID')))
  options(by.col = unique(c(getOption('by.col'),'SUBJID')))
  dsSwissKnife:::.init()
}
