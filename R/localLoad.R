
.load.table<- function(table.expr, mysql.group = 'opal_readonly'){
  is.wholenumber <-
    function(x, tol = .Machine$double.eps^0.5)  abs(x - round(x)) < tol # yanked from the help for is.integer
  mysql.handle <- DBI::dbConnect(RMySQL::MySQL(), group= mysql.group)
  on.exit(DBI::dbDisconnect(mysql.handle))
  table.expr <- sub('.', '_', table.expr, fixed = TRUE) # from 'rhapsody.MH' to 'rhapsody_MH' - the table name in the db
  # set special filters for the present node and table:
  sql <- .set.load.sql(table.expr)
  # tryCatch( ret <-suppressWarnings( DBI::dbGetQuery(mysql.handle ,sql)),
  #            error = function(e){
  #              assign('loaderr', factor(c(rep(sql,6), rep(e$message,6))), envir = .GlobalEnv)
  #             stop(e)
  #              })
  ret <-suppressWarnings( DBI::dbGetQuery(mysql.handle ,sql))
  ret <- ret[, !(grepl('opal', colnames(ret), fixed = TRUE))] # get rid of the metadata columns
  #stringsAsFactors... :
  ret <- as.data.frame(lapply(ret, function(x){
    if (is.character(x)){
      Encoding(x) <- 'latin1' # for funny characters in ...ORRESU
      return(as.factor(x))
    }
    if (is.numeric(x) && !all(is.na(x))) {
      #rmysql returns numerics for integers. Check a sample for integer-ness, if yes, make the whole column integer
      sz <- min(length(x), 200)
      if(all(is.wholenumber(sample(x,sz))) %in% c(TRUE, NA)) {
        return(as.integer(x))
      }
    }
    x
  }))

  convertUnits(ret)
}

localLoadRhapsody<- function(project, tables, mysql.group = 'opal_readonly', cache = TRUE, wide.name ='wide',  value.vars = NULL , widen.formulas = list(),
                      visit.filter = NULL, replace.levels = list() ,by.col = list(), more.cols = list(), col.to.strip = 'VISIT', join.by = NULL, join.type = 'full' ){
  tryCatch({
    if(is.null(value.vars)){
      #return(.load.table(paste0(project, '.', toupper(tables)), mysql.group))
      tb <- .load.table(project, mysql.group)
      return(tb)
    }
    if(cache & file.exists('/var/lib/rserver/work/R/wdRymxdl99')){
      #assign('cache1', factor(rep(Sys.time(),10)), envir = .GlobalEnv)
      load('/var/lib/rserver/work/R/wdRymxdl99')
      # assign('cache2', factor(rep(Sys.time(),10)), envir = .GlobalEnv)

      return(x)
    }
    #assign('nocache1', factor(rep(Sys.time(),10)), envir = .GlobalEnv)

    tables <- .decode.arg(tables)
    value.vars <- .decode.arg(value.vars)
    widen.formulas <- .decode.arg(widen.formulas)
    more.cols <- .decode.arg(more.cols)
    replace.levels <- .decode.arg(replace.levels)
    by.col <- .decode.arg(by.col)
    visit.filter <- .decode.arg(visit.filter)

    chunkie <- function(table){
      # chunk_error <-  factor(rep('no error',6))

      tb <- .load.table(paste0(project, '.', toupper(table)), mysql.group)

      if(col.to.strip %in% colnames(tb)){
        tb <- stripNaCol(tb, col.to.strip, make.unique = TRUE, value.var = value.vars[[table]])
      }

      if(!is.null(visit.filter)){
        if('VISIT' %in% colnames(tb)){
          tb <- tb[tb$VISIT %in% visit.filter, ]
        }
      }

      if(!is.null(widen.formulas[[table]])){
        x <- dsSwissKnife::widen(tb, measure = value.vars[[table]], formula = widen.formulas[[table]], by.col = by.col[[table]] )

      } else {
        x <- tb
      }


      return(list(original = tb, wide = x))


    }

    tablist <-  parallel::mcmapply(chunkie,tables, USE.NAMES= TRUE, SIMPLIFY = FALSE, mc.preschedule = FALSE, mc.cores = 8)
    # tablist <-  sapply(tables, chunkie, simplify = FALSE)
    #return(tablist)
    #assign('tblist', tablist, envir = .GlobalEnv)
    # sometimes if fails in desir, this seems to fix it:
    force(tablist)

    #assign('tablist', c(tablist, list(c(1:6))), envir = .GlobalEnv)
    #names(tablist) <- paste0(wide.name, '_', names(tablist))
    lapply(names(tablist), function(x) {
      assign(paste0(wide.name, '_', x), tablist[[x]]$wide, envir = .GlobalEnv)

      assign(x, tablist[[x]]$original, envir = .GlobalEnv )
    })
    x <- dsSwissKnife::join(paste0(wide.name, '_', names(tablist)), type = join.type, by = join.by)
    save(x, file='/var/lib/rserver/work/R/wdRymxdl99')
    # assign('nocache2', factor(rep(Sys.time(),10)), envir = .GlobalEnv)

    return(x)
  }, error = function(e){
    assign('error', factor(rep(e$message,6)), envir = .GlobalEnv)
    stop(e)})
}

.set.load.sql <- function(table){


  if(!exists('.whoami')){
    return(paste0("select * from ", table))
  }

  ret <- list(
    andis  = list (
      LB =
        paste0("select SUBJID, LBORRES, LBTESTCD, VISIT, LBORRESU from ", table, " where
              ((LBTESTCD='HBA1C' and LBMETHOD='IFCC' and LBORRESU='mmol/mol') or
              (LBTESTCD= 'CPEPTIDE' and LBMETHOD='Cobas') or
              (LBTESTCD='GAD' and LBMETHOD='ELISA') or
              (LBTESTCD not in('HBA1C','CPEPTIDE','GAD'))) and
               VISIT IS NOT NULL and VISIT NOT LIKE '%NA%' and VISIT != '' and LBMETHOD = 'CLINICAL'"),
      VS =
        paste0("select SUBJID, VSORRES, VSTESTCD, VSORRESU from ", table),
      DM =
        paste0("select SUBJID, AGE, SEX from ", table)
    ),

    desir = list(
      #      LB =
      #        paste0("select SUBJID,
      #                 CASE WHEN LBTESTCD = 'HBA1C' THEN (LBORRES - 2.15)*10.929 ELSE LBORRES END as LBORRES,
      #                 LBTESTCD, LBSPEC, VISIT from ", table, " where VISIT IS NOT NULL and VISIT NOT LIKE '%NA%'"),
      LB =
        paste0("select SUBJID,
                 LBORRES,
                 LBTESTCD, LBSPEC, VISIT, LBORRESU from ", table, " where
                 VISIT IS NOT NULL and VISIT NOT LIKE '%NA%' and
                 ((LBTESTCD = 'CREAT' and LBSPEC = 'SERUM') or
                   (LBTESTCD != 'CREAT'))"),

      VS =
        paste0("select SUBJID, VSORRES, VSTESTCD, VISIT, VSORRESU from ", table, " where
               (VSTPT='Measurement 1' or VSTPT IS NULL) and  VISIT IS NOT NULL and VISIT NOT LIKE '%NA%'"),
      DM =
        paste0("select SUBJID, AGE, SEX from ", table)
    ),

    botnia = list(
      LB =
        paste0("select SUBJID, LBORRES, LBTESTCD, LBSPEC, VISIT, LBORRESU from ", table, " where
               ((LBFAST='Y' and (LBTPT='0 min' or LBTPT IS NULL)) and
               ((LBTESTCD='HBA1C' and LBORRESU='mmol/mol') or (LBTESTCD != 'HBA1C')))
               and VISIT IS NOT NULL and VISIT NOT LIKE '%NA%'"),
      VS =
        paste0("select SUBJID, VSORRES, VSTESTCD, VISIT, VSORRESU from ", table, " where
               (VSTPT='Measurement 1' or VSTPT IS NULL) and VISIT IS NOT NULL and VISIT NOT LIKE '%NA%'"),
      DM =
        paste0("select SUBJID, AGE, SEX from ", table)
    ),

    mdc = list(
      LB =
        paste0("select SUBJID,
                 LBORRES,
                 LBTESTCD, LBSPEC, VISIT, LBORRESU from ", table, " where LBFAST = 'Y'and VISIT IS NOT NULL and VISIT NOT LIKE '%NA%'"),

      VS =
        paste0("select SUBJID, VSORRES, VSTESTCD, VISIT, VSORRESU from ", table, " where VISIT IS NOT NULL and VISIT NOT LIKE '%NA%'"),
      DM =
        paste0("select SUBJID, AGE, SEX from ", table)
    ),

    godarts = list(
      # LB =
      #   paste0("select SUBJID, LBORRES, LBTESTCD, VISIT, LBORRESU from ", table, " where
      #          ((LBTESTCD='HBA1C' and LBORRESU='mmol/mol') or
      #          (LBTESTCD != 'HBA1C')) and
      #           VISIT IS NOT NULL and VISIT NOT LIKE '%NA%' and LBMETHOD = 'CLINICAL'"),
      LB =
        paste0("select SUBJID, LBORRES, LBTESTCD, VISIT, 'mmol/mol' as LBORRESU from ", table, " where
              LBTESTCD='HBA1C' and LBORRESU='%' and VISIT IS NOT NULL
              UNION ALL
              select SUBJID, LBORRES, LBTESTCD, VISIT, LBORRESU from ", table, " where
              LBTESTCD != 'HBA1C' and
               VISIT IS NOT NULL and VISIT NOT LIKE '%NA%' and LBMETHOD = 'CLINICAL'"),
      VS =
        paste0("select SUBJID, VSORRES, VSTESTCD, VISIT, VSORRESU from ", table, " where VISIT IS NOT NULL and VISIT NOT LIKE '%NA%'"),
      DM =
        paste0("select SUBJID, AGE, SEX from ", table)


    ),
    dcs = list(
      LB =
        paste0("select SUBJID, LBORRES, LBTESTCD, VISIT, LBORRESU from ", table, " where
              LBFAST ='Y' and VISIT IS NOT NULL and LBMETHOD = 'CLINICAL'"),
      VS =
        paste0("select SUBJID, VSORRES, VSTESTCD, VISIT, VSORRESU from ", table, " where VISIT IS NOT NULL and VISIT NOT LIKE '%NA%'"),
      DM =
        paste0("select SUBJID, AGE, SEX, VISIT from ", table)


    ),
    abos = list(
      LB =
        paste0("select SUBJID, LBORRES, LBTESTCD, LBSPEC, VISIT, LBORRESU from ", table, " where
            (LBTPT='0 min' or LBTPT IS NULL)"),
      VS =
        paste0("select SUBJID, VSORRES, VSTESTCD, VISIT, VSORRESU from ", table, " where VISIT IS NOT NULL and VISIT NOT LIKE '%NA%'"),
      DM =
        paste0("select SUBJID, AGE, SEX from ", table)
    ),
    colaus = list(
      VS =
        paste0("select SUBJID, VSORRES, VSTESTCD, VISIT, VSORRESU from ", table, " where (VSTPT = 'Measurement 1' or VSTPT IS NULL)")

    ),
    provalid = list(
      #  LB =
      #    paste0("select SUBJID, LBORRES, LBTESTCD, VISIT, LBORRESU from ", table),
      LB =
        paste0("select SUBJID, LBORRES, LBTESTCD, VISIT, 'mmol/mol' as LBORRESU from ", table,
               " where LBTESTCD = 'HBA1C'
            UNION ALL
          select SUBJID, LBORRES, LBTESTCD, VISIT,  LBORRESU from ", table,
               " where LBTESTCD = 'CREAT' and LBORRESU = 'umol/L'
            UNION ALL
           select SUBJID, LBORRES, LBTESTCD, VISIT, LBORRESU from ", table,
               " where LBTESTCD != 'HBA1C' and LBTESTCD != 'CREAT'"),

      VS =
        paste0("select SUBJID, VSORRES, VSTESTCD, VISIT, VSORRESU from ", table),
      DM =
        paste0("select SUBJID, SEX from ", table)

    )
  )# ret
  mykey <- gsub('.*_', '', table)
  if(is.null(ret[[.whoami]][[mykey]])){
    return(paste0("select * from ", table))
  } else {
    return(ret[[.whoami]][[mykey]])
  }


}
