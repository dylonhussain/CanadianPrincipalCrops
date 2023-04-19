if (!require(RSQLite)) install.packages('RSQLit====e')
if (!require(tidyverse)) install.packages(tidyverse)

library(RSQLite)
library(tidyverse)
                                                                                #Make connection
conn = dbConnect(RSQLite::SQLite(), 'cropdb.sqlite')                            

                                                                                #Function to run dbexecute and return errors silently
dbExecute2 = function(conn, exp){
  tryCatch({dbExecuteErr <- dbExecute(conn, exp)}, error = function(e){dbExecuteErr <<- e$message})
  return (dbExecuteErr)
}

                                                                                #Drop existing tables so as not to error.
table.list = list('CROP_DATA', 'DAILY_FX', 'FARM_PRICES', 'MONTHLY_FX')
for(table in table.list){
  if(dbExistsTable(conn, table)){
    dbExecute(conn, paste('DROP TABLE ', table))
  }
}

################################################################################ Problem 1 Create tables and inform if error.
                                                                                
db.create.res = list()
db.create.res[1] <- dbExecute2(conn, 
                 "CREATE TABLE CROP_DATA (
                                      CD_ID INTEGER NOT NULL,
                                      YEAR DATE NOT NULL,
                                      CROP_TYPE VARCHAR(20) NOT NULL,
                                      GEO VARCHAR(20) NOT NULL, 
                                      SEEDED_AREA INTEGER NOT NULL,
                                      HARVESTED_AREA INTEGER NOT NULL,
                                      PRODUCTION INTEGER NOT NULL,
                                      AVG_YIELD INTEGER NOT NULL,
                                      PRIMARY KEY (CD_ID)
                                          )"
                 )
 
db.create.res[2]  = dbExecute2(conn, "CREATE TABLE DAILY_FX (
                              DFX_ID INTEGER NOT NULL,
                              DATE DATE NOT NULL,
                              FXUSDCAD FLOAT(6),
                              PRIMARY KEY (DFX_ID)
                              )
                    ")
db.create.res[3]  = dbExecute2(conn, 'CREATE TABLE FARM_PRICES (
                                                CD_ID INTEGER NOT NULL,
                                                DATE DATE NOT NULL,
                                                CROP_TYPE VARCHAR(20) NOT NULL,
                                                GEO VARCHAR(20) NOT NULL,
                                                PRICE_PRERMT FLOAT(6),
                                                PRIMARY KEY (CD_ID)
                                                                )
                               
                               ')
db.create.res[4]  = dbExecute2(conn, 'CREATE TABLE MONTHLY_FX (
                              DFX_ID INTEGER NOT NULL,
                              DATE DATE NOT NULL,
                              FXUSDCAD FLOAT(6),
                              PRIMARY KEY (DFX_ID)
                                                              )'
                               )
err = FALSE
for (res in db.create.res){
  if (!is.numeric(res)){print(res); err = TRUE}
}
if (!err){print('No errors in db creation')}

################################################################################ Problem 2 Read data from web and load to db

cropdf = read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Practice%20Assignment/Annual_Crop_Data.csv')
fxdf = read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Practice%20Assignment/Daily_FX.csv')
mfxdf = read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Monthly_FX.csv')
farmdf = read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Final%20Project/Monthly_Farm_Prices.csv')

dbWriteTable(conn, 'CROP_DATA', cropdf, overwrite = TRUE, header = TRUE)
dbWriteTable(conn, 'DAILY_FX', fxdf, overwrite = TRUE, header = TRUE)
dbWriteTable(conn, 'FARM_PRICES', farmdf, overwrite = TRUE, header = TRUE)
dbWriteTable(conn, 'MONTHLY_FX', mfxdf, overwrite = TRUE, header = TRUE)

################################################################################ Problem 3 Count FARM_PRICES

print(paste('There are ', dbGetQuery(conn, 'SELECT COUNT() FROM FARM_PRICES'), ' records in FARM_PRICES'))

#################################################### ############################ Problem 4 count geographies in FARM_PRICES

print(paste('There are ',dbGetQuery(conn, 'SELECT COUNT(DISTINCT GEO) FROM FARM_PRICES'), ' geographies in FARM_PRICES'))

################################################################################ Problem 5 hectares of rye in canada in 1968

print(paste('There were ',dbGetQuery(conn, 'SELECT SUM(HARVESTED_AREA) FROM CROP_DATA
                        WHERE GEO = "Canada" 
                        AND CROP_TYPE = "Rye"
                        AND strftime("%Y",YEAR) = "1968"
                        '), 
            ' hectares of rye harvested in Canada in 1968'))

################################################################################ Problem 6 first 6 prices of rye.

dbGetQuery(conn, 'SELECT * FROM FARM_PRICES 
                      WHERE CROP_TYPE = "Rye" 
                      LIMIT 6')

################################################################################ Problem 7 Provences that grew barley.
tempdf =(dbGetQuery(conn, 'SELECT DISTINCT GEO FROM FARM_PRICES
                      WHERE CROP_TYPE = "Barley"'))
tempstring = ''
for(element in tempdf){
  tempstring = paste(tempstring, toString(element), " ")
}
print(paste(tempstring, 'grew barley'))

################################################################################ Problem 8 First and last dates of FARM_DATA

dbGetQuery(conn, "WITH TEMP AS (SELECT *, 
                                        ROW_NUMBER() OVER(ORDER BY DATE DESC) AS FIRST_DATE, 
                                        ROW_NUMBER() OVER(ORDER BY DATE ASC) AS LAST_DATE 
                                FROM FARM_PRICES) 
                  SELECT * FROM TEMP
                  WHERE FIRST_DATE=1 OR LAST_DATE=1")


#dbDisconnect(conn)
