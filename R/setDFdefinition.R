#' @export
setDFdefinition<- function (stringsAsFactors = TRUE){
  # trying to force data.frame to load factors:
  #also, starting with version 2.10 of opal dates are no longer loaded as character/vector but as date
  #this breaks stuff in datashield, so fix that too, make dates characters
  myenv <- parent.frame()
  data.frame <- function(...){
    args <- list(...)
    args[['stringsAsFactors']] <- stringsAsFactors
    out <- do.call(base::data.frame, args)
    as.data.frame(sapply(out, function(x){
      if(class(x) %in% c('Date', 'POSIXt')){
        x <- as.character(x)
      }
      x
    },simplify = FALSE))
  }
  assign('data.frame', data.frame, pos = myenv)
  return(TRUE)
}


#' @export
resetDFdefinition <- function(){
  assign('data.frame', base::data.frame, pos = parent.frame())
  return(TRUE)

}
