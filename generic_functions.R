local.postgres.user ='postgres'                         # ******** modificar
local.postgres.password='postgres'                      # ******** modificar
local.postgres.db.name='central'                          # ******** modificar
local.postgres.host='127.0.0.1'                        # ******** modificar
local.postgres.port=5432                                # ******** modificar

#' getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local
#' 
#' @param postgres.user username do postgres
#' @param postgres.password passwd
#' @param postgres.db.name nome da db no postgres
#' @param postgres.host localhost
#' @return FALSE/Con -  (con) retorna um conexao valida  (FALSE) - erro de conexao   
#' @examples 
#' con_local<- getLocalServerCon()
#' 
getLocalServerCon <- function(){
  
  
  status <- tryCatch({
    
    
    # imprimme uma msg na consola
    message(paste0( "Postgres - conectando-se ao servidor Local : ",
                    local.postgres.host, ' - db:',local.postgres.db.name, "...") )
    
    
    
    # Objecto de connexao com a bd openmrs postgreSQL
    con_postgres <-  dbConnect(PostgreSQL(),user = local.postgres.user,
                               password = local.postgres.password, 
                               dbname = local.postgres.db.name,
                               host = local.postgres.host,
                               port = local.postgres.port )
    
    return(con_postgres)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    
    # imprimir msg na consola
    
    message(paste0( "PosgreSQL - Nao foi possivel connectar-se ao host: ",
                    local.postgres.host, '  db:',local.postgres.db.name,
                    "...",'user:',local.postgres.user,
                    ' passwd: ', local.postgres.password)) 
    # guardar o log 
    # if(is.farmac){
    #   saveLogError(us.name = farmac_name,
    #                event.date = as.character(Sys.time()),
    #                action = 'getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local ',
    #                error = as.character(cond$message) )  
    #   
    # } else {
    #   
    #   saveLogError(us.name = main_clinic_name ,
    #                event.date = as.character(Sys.time()),
    #                action = 'getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local',
    #                error = as.character(cond$message) )  
    #}
    
    
    
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  status
}


getPostgresCon <- function(db.name){
  
  local.postgres.db.name <- db.name
  status <- tryCatch({
    
    
    # imprimme uma msg na consola
    message(paste0( "Postgres - conectando-se ao servidor Local : ",
                    local.postgres.host, ' - db:',local.postgres.db.name, "...") )
    
    
    
    # Objecto de connexao com a bd openmrs postgreSQL
    con_postgres <-  dbConnect(PostgreSQL(),user = local.postgres.user,
                               password = local.postgres.password, 
                               dbname = local.postgres.db.name,
                               host = local.postgres.host,
                               port = local.postgres.port )
    
    return(con_postgres)
    
  },
  error = function(cond) {
    
    ## Coisas a fazer se occorre um erro 
    
    # imprimir msg na consola
    
    message(paste0( "PosgreSQL - Nao foi possivel connectar-se ao host: ",
                    local.postgres.host, '  db:',local.postgres.db.name,
                    "...",'user:',local.postgres.user,
                    ' passwd: ', local.postgres.password)) 
    # guardar o log 
    # if(is.farmac){
    #   saveLogError(us.name = farmac_name,
    #                event.date = as.character(Sys.time()),
    #                action = 'getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local ',
    #                error = as.character(cond$message) )  
    #   
    # } else {
    #   
    #   saveLogError(us.name = main_clinic_name ,
    #                event.date = as.character(Sys.time()),
    #                action = 'getLocalServerCon  ->  Estabelece uma conexao com o PostgreSQL Local',
    #                error = as.character(cond$message) )  
    #}
    
    
    
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # Do nothing
  })
  
  status
}

count_dispense_us <- function(df_last_dispense, tipo_dispensa ){
  
  if(!is.na(df_last_dispense$mainclinicname[1])){
    
    us <- df_last_dispense$mainclinicname[1]
    index  <- which(df_modelos_farmac$us==us)
    
    if(tipo_dispensa=="dispensa_mensal"){
      
      df_modelos_farmac$dispensa_mensal[index] <<-  df_modelos_farmac$dispensa_mensal[index] +1
    }
    else if (tipo_dispensa=="dispensa_trimestral"){
      
      
      df_modelos_farmac$dispensa_trimestral[index] <<-  df_modelos_farmac$dispensa_trimestral[index] +1
      
    }
    else if (tipo_dispensa=="dispensa_covid"){
      
      df_modelos_farmac$dispensa_covid_19[index] <<-  df_modelos_farmac$dispensa_covid_19[index] +1
      
    } else {
      message(paste0("No dispense type provided for dispense : ", patient_id))
    }
    
  } 
  else if(!is.na(df_last_dispense$sync_temp_dispenseid[1])) {
    
    us <- df_last_dispense$sync_temp_dispenseid[1]
    index  <- which(df_modelos_farmac$us==us)
    if(tipo_dispensa=="dispensa_mensal"){
      
      df_modelos_farmac$dispensa_mensal[index] <<-  df_modelos_farmac$dispensa_mensal[index] +1
    }
    else if (tipo_dispensa=="dispensa_trimestral"){
      
      
      df_modelos_farmac$dispensa_trimestral[index] <<-  df_modelos_farmac$dispensa_trimestral[index] +1
      
    }
    else if (tipo_dispensa=="dispensa_covid"){
      
      df_modelos_farmac$dispensa_covid_19[index] <<-  df_modelos_farmac$dispensa_covid_19[index] +1
      
    } else {
      message(paste0("No dispense type provided for dispense : ", patient_id))
    }
    
  }
  else{
    message(paste0(" No mainclinicname for dispense :", patient_id))
  }
}

getNidByUuid <- function(uuid, df){
  
  index <- which(df$uuidopenmrs==uuid)
  nid <- NA
  if(length(index)>=1){
    index=index[1]
    nid <- df$patientid[index]
    
  } 
  
  
}
