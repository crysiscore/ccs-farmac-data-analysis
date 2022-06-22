
require(RPostgreSQL)
require(plyr)     ## instalar com install.packages("plyr")
require(dplyr)    ## instalar com install.packages("dplyr")
library(xlsx)
####################################### Configuracao de Parametros  #####################################################################


wd <- '~/Git/farmac_data_analisys/'  
setwd(wd)
source('generic_functions.R')


# Objecto de connexao com a bd openmrs postgreSQL
con_postgres <-  getLocalServerCon()


sync_patients   <-  dbGetQuery(con_postgres, "select * from sync_temp_patients")
sync_dispenses  <-  dbGetQuery(con_postgres,  "select * from sync_temp_dispense")
sync_episodes   <- dbGetQuery(con_postgres,  "select * from sync_temp_episode")
sync_episodes$patientid <- lapply(sync_episodes$patientuuid, sync_patients,  FUN = getNidByUuid)

sync_patients %>% group_by(mainclinicname)  %>% tally()
sync_patients %>% group_by(mainclinicname)  %>% summarise(n=n())

last_year_pat<- filter(sync_patients, as.Date(sync_patients$prescriptiondate) >= '2020-09-21' & as.Date(sync_patients$prescriptiondate) <= '2021-09-20')

wout_dispense <- filter(last_year_pat, ! patientid %in% sync_dispenses$patientid)

wout_dispense <- filter(wout_dispense, ! patientid %in% sync_episodes$patientid)


stats =wout_dispense %>% group_by(mainclinicname, clinicname)  %>% tally() %>% rename(centro_de_saude=mainclinicname, farmacia_privada=clinicname,total=n)
print(stats)

require(openxlsx)

wout_dispense$nome_completo <- paste0(wout_dispense$firstnames, ' ', wout_dispense$lastname)

lista <- wout_dispense[,c("patientid","nome_completo","sex","mainclinicname","clinicname","prescriptiondate")] %>%
              rename(nid=patientid, centro_de_saude=mainclinicname, farmacia_privada=clinicname,data_referencia= prescriptiondate)



list_of_datasets <- list("referido_sem_dispensa" = stats, "lista" = lista)
write.xlsx(list_of_datasets, file = "pacientes_referidos_sem_dispe.xlsx")




