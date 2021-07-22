convertUnits <- function(df){

  if(!is.data.frame(df)){
    df <- get(df, envir = parent.frame())
  }
  if ('LBTESTCD' %in% colnames(df)){
    test.col <- 'LBTESTCD'
    unit.col <- 'LBORRESU'
    value.col <- 'LBORRES'
  } else if ('VSTESTCD' %in% colnames(df)){
    test.col <- 'VSTESTCD'
    unit.col <- 'VSORRESU'
    value.col <- 'VSORRES'

  } else {
    return(df)
  }
  if('STUDYID' %in% colnames(df)){
    cohort <- tolower(sqldf::sqldf(paste0('select STUDYID from df limit 1'))[1,])
  } else if (exists('.whoami', envir = .GlobalEnv)){
    cohort <- .whoami
  } else {
    return(df)
  }


  conversion_table <- conversionRhapsody[tolower(conversionRhapsody$COHORT_OPAL )== tolower(cohort), ]
  updater <- paste0("select 'update df set ", value.col, "  =  ' ||  TARGET_CONVERSION_FACTOR || ', ", unit.col, " = ''' || TARGET_UNIT || ''', ",
                    test.col, " = '''  || TARGET_TESTCD || ''' where  ", test.col, " = ''' || TESTCD_IN_DB || ''' and ", unit.col, " = ''' || UNIT_IN_DB || '''' from conversion_table where TARGET_CONVERSION_FACTOR LIKE '%", value.col, "%'")

  sqls <- c('PRAGMA encoding = "UTF-8"',sqldf::sqldf(updater)[,1], paste0('select * from df', NULL)) # pragma is there to deal with µmol/l
  out <- sqldf::sqldf(sqls)

  #sqldf converts some factors to character vectors, convert them back, also reset the levels:
  as.data.frame(lapply(out, function(x){
    if (is.character(x) | is.factor(x)){
      return(as.factor(x))
    }
    x
  }))

}
