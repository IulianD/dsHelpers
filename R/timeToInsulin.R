#' @export
timeToInsulin <- function(arglist){
  do.call(.time.to.insulin,.decode.arg(arglist))
}


.time.to.insulin <- function(treatment.df, lab.df, diag.df,  treatments = list(CMCAT = "INSULINS AND ANALOGUES"), trt.date.col = 'CMSTDTC', lab.date.col = 'LBDTC', diag.date.col = 'MHDTC',
                             occur.col  =  list(CMOCCUR = list(yes = 'Y', no = 'N')),  diag.occur.col = list(MHOCCUR = list(yes = 'Y', no = 'N')), diag.visit = 'DIAGNOSIS',
                             diagnosis = list(MHTERM = "TYPE 2 DIABETES"), min.treatment.days = 180,
                             labtest = list(LBTESTCD = 'HBA1C'), min.measurement = list(LBORRES = 69), measure = list(LBORRESU = "mmol/m"), min.days.apart = 90, criteria = 'both'){
  cat.col <- names(treatments)[1]
  cat.val <- paste0('("',paste(treatments[[cat.col]], collapse = '","'), '")') # 'IN' expression  e.g ("something", "something else")
  test.name <- names(labtest)[1]
  test.val <- labtest[[1]]
  measure.name <- names(measure)[1]
  measure.val <- measure[[1]]
  measurement.name <- names(min.measurement)[1]
  measurement.val <- min.measurement[[1]]

  occur <- names(occur.col)[1]
  yes <- occur.col[[occur]][['yes']]
  no <- occur.col [[occur]][['no']]
  diag.occur <- names(diag.occur.col)[1]
  diag.yes <- diag.occur.col[[diag.occur]][['yes']]
  diag.no <- diag.occur.col [[diag.occur]][['no']]
  diag.col <- names(diagnosis)[1]
  diag.val <- diagnosis[[1]]

  query.map <- list(
    first = 'select * from first_insulin',
    second = 'select * from first_hba1c',
    both = 'select * from first_insulin
                  union all /* do not bother with distinct, we are going to group in a moment */
                select * from first_hba1c
                '
  )

  sql <- c( # criterion i) starting sustained( more than min.treatment.days) insulin treatment:
    paste0('create table cy as select * from (select c1.subjid, c1.', occur, ', c1.' ,trt.date.col, ' as dt1, c2.', trt.date.col, ' as dt2 from
           `',treatment.df, '` c1 inner join `', treatment.df, '` c2 on(c1.subjid = c2.subjid and date(c2.',trt.date.col,') > date(c1.',trt.date.col,'))
           where c1.', cat.col, ' in ', cat.val, ' and c1.', trt.date.col, ' is not null and c1.', occur, ' = "', yes, '" and c2.', cat.col, ' in ', cat.val, '  and c2.', trt.date.col,
           ' is not null and c2.', occur,' = "', yes, '")
           where julianday(dt2) - julianday(dt1) >= ', min.treatment.days),
    paste0('create table cn as select distinct subjid, ', trt.date.col, ' from `', treatment.df, '` where ', cat.col, ' in ', cat.val, ' and ', occur, ' = "', no, '"'),
    paste0('create index cnidx on cn(subjid)'),
    paste0('create view first_insulin as select subjid, min(dt1) as first_date from (
           with mysel as (select min(date(c3.', trt.date.col, ')) from cn c3 where c3.subjid = c1.subjid  and date(c3.', trt.date.col, ') >date(c1.dt1))
           select  subjid, dt1, dt2, julianday(dt2) - julianday(dt1) as days from
           (select c1.subjid as subjid, c1.cmoccur as cmoccur, c1.dt1 as dt1,max(date(c1.dt2)) as dt2
           from cy c1 where date(c1.dt2) <= coalesce((select * from mysel) ,date(c1.dt2)) group by subjid, date(c1.dt1)) where dt2 is not null)
           group by subjid'),
    # criterion ii) 2 or more HbA1c measurements > min.measurement more than min.days.apart apart when on 2 or more non-insulin diabetes therapies
    # first filter down the original data frame:
    paste0('create table d_temp as select SUBJID, ', lab.date.col, ' from `', lab.df, '` where ', test.name, ' =  "',test.val, '" and ', measure.name, ' = "',measure.val, '"  and ',
           measurement.name, ' >= ', measurement.val) ,
    # join with itself to create intervals (only keep intervals larger than min.time.apart days)
    # then join with the medication data frame, bring all the allowed treatments, make sure cmoccur = 'Y'
    # then select the earliest date with count(distinct treatment) >= 2
    paste0('create view first_hba1c as select subjid, min(dt1) as first_date from
                    (select subjid, dt1, dt2,  count(distinct treatment)  from
                      (select d.*, c.',trt.date.col,' as dt_treatment, c.cmcat as treatment, c.cmoccur from
                        (select d_now.subjid, d_now.', lab.date.col, ' as dt1, d_later.', lab.date.col, ' as dt2 from d_temp d_now inner join d_temp d_later
                          on (d_now.subjid = d_later.subjid
                              and d_later.', lab.date.col, ' > d_now.', lab.date.col,
           ' and julianday(d_later.', lab.date.col, ') - julianday(d_now.', lab.date.col,') >= ', min.days.apart, ')) d
                        inner join `',treatment.df, '` c
                        on (d.subjid = c.subjid and julianday(c.', trt.date.col ,') between (julianday(d.dt1) - 30) and (julianday(d.dt2) + 30))
                        where c.', cat.col, ' not in ', cat.val,' and c.', occur, ' = "', yes, '")
                    group by subjid, dt1, dt2 having count(distinct treatment) >= 2)
                   group by subjid'),
    # now union the 2 views (based on the 2 criteria) and extract the earliest dates per participant
    paste0('with firsts as (select subjid, min(first_date) as first_date from
                (', query.map[[criteria]],') group by subjid)
     select * from (select diag.subjid as SUBJID, julianday(firsts.first_date) - min(julianday(', diag.date.col,')) as DAYS_TO_INSULIN, 1 as "CASE" from
      firsts inner join `', diag.df, '` diag on (firsts.subjid = diag.subjid)
       where diag.',diag.col,' = "', diag.val, '"
      and diag.', diag.occur, ' = "', diag.yes, '"
      and diag.visit = "',diag.visit, '"
      group by diag.subjid) where days_to_insulin >=0')

  )

  # simples :)

  sqldf::sqldf(sql)


}


#' @export
monotherapyNoInsulin <- function(arglist){
  do.call(.monotherapy.no.insulin,.decode.arg(arglist))
}
.monotherapy.no.insulin <- function(treatment.df, diag.df, insulin.treatments = list(CMCAT = "INSULINS AND ANALOGUES"), trt.date.col = 'CMSTDTC', diag.date.col = 'MHDTC',
                                    occur.col  =  list(CMOCCUR = list(yes = 'Y', no = 'N')),  diag.occur.col = list(MHOCCUR = list(yes = 'Y', no = 'N')), diag.visit = 'DIAGNOSIS',
                                    diagnosis = list(MHTERM = "TYPE 2 DIABETES"), years.after.diagnosis = 5, days.offset = 30){
  cat.col <- names(insulin.treatments)[1]
  cat.val <- paste0('("',paste(insulin.treatments[[cat.col]], collapse = '","'), '")') # 'IN' expression  e.g ("something", "something else")

  occur <- names(occur.col)[1]
  yes <- occur.col[[occur]][['yes']]
  no <- occur.col [[occur]][['no']]
  diag.occur <- names(diag.occur.col)[1]
  diag.yes <- diag.occur.col[[diag.occur]][['yes']]
  diag.no <- diag.occur.col [[diag.occur]][['no']]
  diag.col <- names(diagnosis)[1]
  diag.val <- diagnosis[[1]]

  sql <- c(paste0('create index cx on ',treatment.df,'(subjid,', cat.col, ')'),

           paste0('select subjid, min(diff_days) as diff_days, 0 as "case" from
                  (select subjid, diff_days from
                    (select c1.subjid as subjid, c1.', cat.col, ' as treatment, julianday(c1.', trt.date.col, ') - julianday(d.', diag.date.col, ') as diff_days, c1.', trt.date.col, ' as dt_treatment from ', treatment.df, ' c1 inner join ',
                  diag.df, ' d on (c1.subjid = d.subjid and julianday(c1.', trt.date.col, ') - julianday(d.', diag.date.col, ') >= ', years.after.diagnosis * 365 - days.offset,')
                      where c1.', cat.col, ' not in ', cat.val,' and c1.', occur, ' = "', yes, '" and not exists
                        (select 1 from ', treatment.df, ' c2 where c2.', cat.col,' in ', cat.val,
                  ' and c2.subjid = c1.subjid
                          /* and julianday(c2.', trt.date.col, ') between julianday(c1.', trt.date.col, ') - ', days.offset, ' and julianday(c1.', trt.date.col, ') + ', days.offset,
                  'and julianday(c2.', trt.date.col, ') <= julianday(d.', diag.date.col, ') + ', years.after.diagnosis * 365 - days.offset,
                  '*/ and c2.',occur, ' = "', yes, '")
                      and d.', diag.col, ' = "', diag.val, '" and d.', diag.occur, ' = "', diag.yes, '" and d.visit = "',diag.visit, '")
                   group by subjid, diff_days having count(distinct treatment) = 1)
                 group by subjid'))


  sql <- c(paste0('create index cx on ',treatment.df,'(subjid,', cat.col, ')'),

           paste0('create table ctrl_sqldf as select c1.subjid as subjid, c1.', cat.col, ' as treatment, julianday(c1.', trt.date.col, ') - julianday(d.', diag.date.col, ') as diff_days, c1.', trt.date.col, ' as dt_treatment from ', treatment.df, ' c1 inner join ',
                  diag.df, ' d on (c1.subjid = d.subjid and julianday(c1.', trt.date.col, ') - julianday(d.', diag.date.col, ') >= ', years.after.diagnosis * 365 - days.offset,')
                      where c1.', cat.col, ' not in ', cat.val,' and c1.', occur, ' = "', yes, '"
                          and c1.', cat.col, ' in ( "BLOOD GLUCOSE LOWERING DRUGS, EXCL. INSULINS" ,"DRINK AND FOOD" )
                          and not exists
                        (select 1 from ', treatment.df, ' c2 where c2.', cat.col,' in ', cat.val,
                  ' and c2.subjid = c1.subjid
                          and c2.',occur, ' = "', yes, '" and julianday(c2.', trt.date.col, ') - julianday(d.', diag.date.col, ') <= ', years.after.diagnosis * 365 - days.offset,' )
                      and d.', diag.col, ' = "', diag.val, '" and d.', diag.occur, ' = "', diag.yes, '" and d.visit = "',diag.visit, '"'),
           paste0('select subjid as SUBJID, min(diff_days) as DIFF_DAYS, 0 as "CASE" from ctrl_sqldf where subjid not in (select subjid  from ctrl_sqldf group by subjid, diff_days having count(distinct treatment) >=2) group by subjid ')
  )
  sqldf::sqldf(sql)
  #return(sql)

}


