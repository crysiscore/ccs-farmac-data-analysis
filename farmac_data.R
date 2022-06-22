# Packages que contem algumas funcoes a serem usadas 
require(RPostgreSQL)
require(plyr)     ## instalar com install.packages("plyr")
require(dplyr)    ## instalar com install.packages("dplyr")
library(writexl)
####################################### Configuracao de Parametros  #####################################################################
#########################################################################################################################################
wd <- '~/Git/farmac_data_analisys/'  
setwd(wd)


farmac.postgres.user ='postgres'                         # ******** modificar
farmac.postgres.password='iD@rt2020'                   # ******** modificar
farmac.postgres.db.name='central'                        # ******** modificar
farmac.postgres.host='192.168.1.163'                   # ******** modificar
farmac.postgres.port=5432                              # ******** modificar

# Objecto de connexao com a bd openmrs postgreSQL
# Objecto de connexao com a bd openmrs postgreSQL
con_postgres <-  dbConnect(PostgreSQL(), user = farmac.postgres.user,
                           password = farmac.postgres.password, 
                           dbname = farmac.postgres.db.name,
                           host = farmac.postgres.host,
                           port = farmac.postgres.port )

##################################################################################

sync_patients   <-  dbGetQuery(con_postgres, "select * from sync_temp_patients")
sync_dispenses  <- dbGetQuery(con_postgres, "select * from sync_temp_dispense")

sync_patients %>% group_by(mainclinicname) %>% tally()
sync_patients %>% group_by(mainclinicname) %>% summarise(n=n())

export_sync_patients = sync_patients %>% 
  select(patientid,firstnames, lastname,sex,datainiciotarv,mainclinicname,clinicname) %>% 
  arrange(desc(mainclinicname))

export_sync_dispense = sync_dispenses %>% arrange(patientid, desc( dispensedate))  %>% 
  distinct(patientid, .keep_all = TRUE) %>% 
  select(patientid, patientfirstname, patientlastname, regimenome, dispensedate, dateexpectedstring,mainclinicname)


export_sync_dispense = export_sync_dispense %>% left_join(export_sync_patients, by = 'patientid')  %>% 
  select(patientid, patientfirstname, patientlastname,sex, datainiciotarv,regimenome, dispensedate, dateexpectedstring,mainclinicname.y, mainclinicname.x, clinicname)

names(export_sync_dispense)[7] = "data_ult_levant"
names(export_sync_dispense)[8] = "data_prox_levant"
names(export_sync_dispense)[9] = "us_referencia"
names(export_sync_dispense)[11] = "farmac"


length(which(! export_sync_dispense$patientid %in% export_sync_patients$patientid))

temp=export_sync_dispense[which(!export_sync_dispense$patientid %in% export_sync_patients$patientid),]



names(temp)[2] = 'firstnames'
names(temp)[3] = 'lastname'
names(temp)[5] = 'clinicname'


a=rbind.fill(export_sync_patients,temp)


a=rbind.fill(export_sync_patients,temp)

table(sync_patients$clinicname)
table(sync_patients$mainclinicname)


