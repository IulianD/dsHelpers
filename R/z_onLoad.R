.onLoad <- function(...){
  options(hidden.fields = c(getOption('hidden.fields'),'SUBJID', 'USUBJID'))

  dsSwissKnife:::.init()
}
