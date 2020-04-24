.onLoad <- function(...){
  if(is.null(getOption('hidden.fields'))){
    options(hidden.fields = c(getOption('hidden.fields'),'SUBJID', 'USUBJID'))
  }
  dsSwissKnife:::.init()
}
