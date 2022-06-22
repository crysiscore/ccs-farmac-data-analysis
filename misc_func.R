getPatientInfo <- function(con.postgres) {
  
  
  patients  <-
    dbGetQuery(
      con.postgres,
      paste0(
        "select pat.id, pat.patientid,dateofbirth::TIMESTAMP::DATE as dateofbirth,lower(pat.firstnames) as firstnames , 
          pat.sex, lower(pat.lastname) as lastname , ep.startreason,ep.data_referencia
          from patient pat left join
          (
           select patient, max(startdate) as data_referencia, startreason
             from episode
              group by patient, startreason
  
          )  ep on ep.patient = pat.id and  ep.startreason ='Referido para outra Farmacia'
  
          left join (
              select patientid, count(*) as total
              from packagedruginfotmp
              group by patientid
         ) dispensas on dispensas.patientid = pat.patientid
  
               left join (
              select patientid, max(dispensedate) as ult_levant_idart
              from packagedruginfotmp
              group by patientid
         ) ult_lev on ult_lev.patientid = pat.patientid
  


  
   "
      )
    )
  
  patients  # same as return(patients)
  
}

getDataReferecia <- function(con.postgres, patient.id){
  data  <-
    dbGetQuery(
      con.postgres,
      paste0("select patientid,e.startreason,e.stopreason,e.startdate,e.stopdate from  patient p   inner join episode e on  e.patient=p.id   where patientid='", patient.id,"' ;"))
  data
}