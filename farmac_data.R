# Packages que contem algumas funcoes a serem usadas 
require(RMySQL)
require(dplyr)    ## instalar com install.packages("dplyr")
#library(writexl)
####################################### Configuracao de Parametros  #####################################################################
#########################################################################################################################################
wd <- "/Users/asamuel/Projects/ccs-farmac-data-analysis"
setwd(wd)



##################################################################################


sync_patients   <-  read.csv('data/sync_temp_patients.csv', sep = ',', header = TRUE)
sync_dispenses  <- read.csv('data/sync_temp_dispense.csv', sep = ',', header = TRUE)


referidos <- read.csv('data/referidos.csv', sep = ';', header = TRUE)
voltaram <- read.csv('data/voltara_referencia.csv', sep = ';', header = TRUE) %>% mutate(data_retorno_us = Data.de.Retorno.a.US)

# change column name NID.1 to patientid in df voltaram using dplyr functions
voltaram <- voltaram %>% rename(patientid = NID.1, us = NID)

# filter unique patientid in voltaram keepoing the most recent data_retorno_us
voltaram <- voltaram %>% arrange(patientid, desc(data_retorno_us)) %>%
  distinct(patientid, .keep_all = TRUE)


# Select columns patientid, firstnames, lastname, sex, pickupdate, dispensedate, dateexpectedstring, mainclinicname, syncstatus, uuidopenmrs,
# from sync_temp_dispense

filtered_dispense <- sync_dispenses %>% 
  select(patientid, patientfirstname, patientlastname, dispensedate, dateexpectedstring, mainclinicname, syncstatus, uuidopenmrs) %>%
  arrange(desc(dispensedate))

# Join sync_patients with filtered_dispense on patientid to get details : clinicname, estadopaciente

patient_dispense <- filtered_dispense %>% left_join(sync_patients %>% select(patientid, clinicname, estadopaciente), by = 'patientid')


# Ordena por patientid e dispensedate (mais recente primeiro), depois remove duplicados
unique_patient_dispense <- patient_dispense %>%
  arrange(patientid, desc(dispensedate)) %>%
  distinct(patientid, .keep_all = TRUE)

# convert dispensedate and dateexpectedstring to Date format
unique_patient_dispense$dispensedate <- as.Date(unique_patient_dispense$dispensedate, format = "%Y-%m-%d")
unique_patient_dispense$dateexpectedstring <- as.Date(unique_patient_dispense$dateexpectedstring, format= "%d %b %Y")


# Rename some column in unique_patient_dispense 

unique_patient_dispense <- unique_patient_dispense %>% rename(unidade_sanitaria = mainclinicname, 
                                                              farmacia_privada = clinicname,
                                                              data_ultima_dispensa_fp = dispensedate,
                                                              data_prox_lev_fp = dateexpectedstring) %>% mutate( nome_completo_fp = paste(patientfirstname, patientlastname))

# Drop columns syncstatus, patientfirstname, patientlastname, uuidopenmrs
unique_patient_dispense <- unique_patient_dispense %>% select(, -patientfirstname, -patientlastname)

# Actualmente em tarv df

vec_db_names <- c("catembe","1_maio","bagamoio","maxaquene","chamanculo","porto","xipamanine","albasine","altomae","polana_canico","josemacamo_cs","zimpeto")

# Loop over vec_db_names and get data from each db and combine into one df actual_tarv, add a column db_name to identify the db
actual_tarv <- data.frame()
for(db_name in vec_db_names){
  db_connection <- getOpenMRSCon(db.name = db_name)
  temp_df <- getAllPatientsOpenMRS(db_connection)
  #if temp_df is empty, next
  if(nrow(temp_df) == 0){
    # Message: No data in db db_name
    message(paste0("No data in db ", db_name))
    dbDisconnect(db_connection)
    next
  }
  # add a column db_name to temp_df
  temp_df$db_name <- db_name
  actual_tarv <- rbind(actual_tarv, temp_df)
  # disconnect from db
  dbDisconnect(db_connection)
}

# Rename some column in actual_tarv
openmrs_tarv <- actual_tarv %>% rename( patientid =  identifier,data_ult_lev_us = data_ult_levantamento, data_prox_lev_us = proximo_marcado , nome_completo_openmrs = nome_completo) %>% select(-patient_id)
                                  

# Join unique_patient_dispense with actual_tarv on patientid
# add all  columns  

pacientes_dd <- unique_patient_dispense %>% left_join(openmrs_tarv, by = 'patientid')



# create a column 'notas'which will have the following values:
# join with voltaram df on patientid
# if patientid in voltaram$patientid  -> 'Voltaram a referencia'

#pacientes_dd <- pacientes_dd %>% mutate(notas = ifelse(patientid %in% voltaram$patientid, 'Voltaram a referencia', ''))

pacientes_dd <- pacientes_dd %>% left_join(voltaram %>% select(patientid, data_retorno_us), by = 'patientid') %>% 
  mutate(notas = ifelse(!is.na(data_retorno_us), paste0( 'Voltaram a referencia em ', data_retorno_us), ""))


pacientes_dd$data_estado <- as.Date(pacientes_dd$data_estado, format = "%d/%m/%Y")
pacientes_dd$data_ult_consulta <- as.Date(pacientes_dd$data_ult_consulta, format = "%d/%m/%Y")
pacientes_dd$data_prox_marcado <- as.Date(pacientes_dd$data_prox_marcado, format = "%d/%m/%Y")
pacientes_dd$data_prox_lev_us <- as.Date(pacientes_dd$data_prox_lev_us, format = "%d/%m/%Y")
pacientes_dd$data_ult_lev_us <-  as.Date(pacientes_dd$data_ult_lev_us, format = "%d/%m/%Y")

# remove timestamp frorm data_retorno_us using substr
pacientes_dd$data_retorno_us <- substr(pacientes_dd$data_retorno_us, 1, 10)
pacientes_dd$data_retorno_us <- as.Date(pacientes_dd$data_retorno_us, format = "%Y-%m-%d")

# Add Column estado_levantamento
# if (data_prox_lev_us + 28) < Sys.Date() -> 'Faltoso_US'

# if (data_prox_lev_fp + 28) < Sys.Date() and (data_prox_lev_us + 28) < Sys.Date() -> 'Faltoso_us_FP'
#else if (data_prox_lev_fp + 28) >=  Sys.Date() and (data_prox_lev_us + 28) >= Sys.Date() -> 'Activo_us_FP'
# else if (data_prox_lev_fp + 28) < Sys.Date() -> 'Faltoso_FP'
# else if (data_prox_lev_us + 28) >= Sys.Date() -> 'Activo_US'
# else if (data_prox_lev_fp + 28) >= Sys.Date() -> 'Activo_FP'
# else if (data_prox_lev_us + 59) < Sys.Date() -> 'Abandono'

pacientes_dd <- pacientes_dd %>% mutate(estado_levantamento = case_when(

  !is.na(data_prox_lev_fp) & (data_prox_lev_fp + 28) < Sys.Date() & (!is.na(data_prox_lev_us) & (data_prox_lev_us + 28) < Sys.Date()) ~ 'Faltoso_US_FP',
  !is.na(data_prox_lev_fp) & (data_prox_lev_fp + 28) >= Sys.Date() & (!is.na(data_prox_lev_us) & (data_prox_lev_us + 28) >= Sys.Date()) ~ 'Activo_US_FP',
  !is.na(data_prox_lev_fp) & (data_prox_lev_fp + 28) >= Sys.Date() & (!is.na(data_prox_lev_us) & (data_prox_lev_us + 28) < Sys.Date()) ~ 'Activo_FP_Faltoso_US',
  !is.na(data_prox_lev_fp) & (data_prox_lev_fp + 28) < Sys.Date()  & (!is.na(data_prox_lev_us) & (data_prox_lev_us + 28) >= Sys.Date()) ~ 'Activo_US_Faltoso_FP',
  
  !is.na(data_prox_lev_us) & (data_prox_lev_us + 59) < Sys.Date() &(data_prox_lev_fp + 59) < Sys.Date() ~ 'Abandono',
  TRUE ~ 'Desconhecido'
) )



## Filter pacientes  who are no longer in FP
##  estado_levantamento = 'Desconhecido' with !is.na(data_retorno_us)
##  estado_levantamento = 'Activo_US_FP' with !is.na(data_retorno_us)
##  estado_levantamento = 'Activo_US_Faltoso_FP' with !is.na


# Filter activos na FP with estado_levantamento "Activo_FP" or "Activo_us_FP"