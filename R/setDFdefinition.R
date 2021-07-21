#' @export
setDFdefinition<- function (stringsAsFactors = TRUE, cohort_name = NULL){
  # trying to force data.frame to load factors:
  #also, starting with version 2.10 of opal dates are no longer loaded as character/vector but as date
  #this breaks stuff in datashield, so fix that too, make dates characters
  myenv <- parent.frame()
  data.frame <- function(...){
    args <- list(...)
    args[['stringsAsFactors']] <- stringsAsFactors
    out <- do.call(base::data.frame, args)
    if(stringsAsFactors)
    	ret <- as.data.frame(sapply(out, function(y){
      		if(length(intersect(class(y) , c('character', 'Date', 'POSIXct', 'POSIXlt', 'POSIXt'))) >0 ){
        		return(factor(y))
      		} else {
        		return(y)
      		}
    	},simplify = FALSE))
    return(ret)
  }
  assign('data.frame', data.frame, pos = myenv)
  if(!is.null(cohort_name){
	     assign('whoami', cohort_name, pos = myenv)
  }
  return(TRUE)
}


#' @export
resetDFdefinition <- function(){
  assign('data.frame', base::data.frame, pos = parent.frame())
  return(TRUE)

}
