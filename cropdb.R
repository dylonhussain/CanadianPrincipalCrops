if (!require(RSQLite)) install.packages('RSQLite')
if (!require(tidyverse)) install.packages(tidyverse)

library(RSQLite)
library(tidyverse)
                                                                                #Load data from interwebs to data frames
cropdf = read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Practice%20Assignment/Annual_Crop_Data.csv')
fxdf = read.csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-RP0203EN-SkillsNetwork/labs/Practice%20Assignment/Daily_FX.csv')

summary(cropdf)
summary(fxdf)

conn = dbConnect(RSQLite::SQLite(), 'cropdb.sqlite')                            

dbExecute2 = function(conn, exp){
  tryCatch({dbExecuteErr <- dbExecute(conn, exp)}, error = function(e){dbExecuteErr <<- e$message})
  return (dbExecuteErr)
}

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
err = FALSE
for (res in db.create.res){
  if (!is.numeric(res)){print(res); err = TRUE}
}
if (!err){print('No errors in db creation')}
