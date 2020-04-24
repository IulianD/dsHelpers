#' @export

stripNaCol <- function(df, col, make.unique = FALSE, value.var = NULL, remove.double.measures = TRUE){
  # ugly fix for 'NA' as text in the database - to be removed!!!
  out <- df
  out[out == 'NA' | out =='VISIT NA'] <- NA
  # another one for 'VISIT NA' in dcs:

  out <- out[!is.na(out[[col]]),]
  if(nrow(out) < dsSwissKnife:::dsBase_setFilterDS()){
    out[] <- NA
  }
  out <- droplevels(out)
  if (make.unique){
    out <- unique(out)
  }
  if(!is.null(value.var)){
    out <- out[!is.na(out[[value.var]]),]
    # another ugly trick: remove double measures for the same subject + same everything else
    # but possibly different value.vars

    if(remove.double.measures){
      out <- out[rownames(unique(out[,!(colnames(out) %in% value.var)])),]
    }
  }
  out
}

