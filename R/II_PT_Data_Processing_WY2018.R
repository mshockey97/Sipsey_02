#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name: Initial PT Data Processing
# Coder: C. Nathan Jones
# Date: 16 April 2019
# Purpose: Process PT Data collected across the Palmer Lab Delmarva wetland sites
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Note for next time [4/30/2019 19:42]-----------------------------------------------
#Check into jump at DK on  10/6/2018 -- I think I changed the fihsing line here
#Check into jump in BB at 7/24/2018
#CHeck abnormal jump in ND sites on 2/10. Did I rehang them then?
#Water level Estimate for DF, JA, JC
#Sears Sites

#More Notes 5/9
#Gregout 4/16? Maybe merge with Dogbone outlet?
#Jones Road South.  Find missing summer files. 
#Did I rehang Tiger Paw Out on 12/20


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#1.0 Setup Worskspace---------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#clear environment
remove(list=ls())

#load relevant packages
library(xts)
library(dygraphs)
library(parallel)
library(devtools)
devtools::install_github("khondula/rodm2")
library(RSQLite)
library(DBI)
library(rodm2)
library(zoo)
library(lubridate)
library(readxl)
library(tidyverse)

#Read custom R functions
source("functions/download_fun.R")
source("functions/dygraph_ts_fun.R")
source("functions/waterHeight_fun.R")
source("functions/baro_fun.R")
source("functions/db_get_ts.R")
source("functions/offset_fun.R")

#Define working dir
working_dir<-"//nfs/palmer-group-data/Choptank/Nate/PT_Data/"

#Set system time zone 
Sys.setenv(TZ="America/New_York")

#Define database connection
db<-dbConnect(RSQLite::SQLite(),"//nfs/palmer-group-data/Choptank/Nate/PT_Data/choptank.sqlite")

#Download survey data 
survey<-read_csv(paste0(working_dir,"survey_data/survey_V1.csv"))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#2.0 Create lookup table for PT data files------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#2.1 Compile a list of file paths---------------------------------------------------
#Identify files with HOBO export files
files<-list.files(working_dir, recursive = T)
files<-files[substr(files,nchar(files)-2,nchar(files))=="csv"] 
files<-files[grep(files,pattern = "export")]

#Remove non-relevant files 
files<-files[-grep(files,pattern = 'archive')]

#Create function to retrieve info from each file
file_fun<-function(n){
  
  #Download data
  temp<-read_csv(paste0(working_dir,files[n]), skip=1)
  
  #Determine serial number
  serial_number<-colnames(temp)[grep("LGR",colnames(temp))][1]  #Find collumn name with serial number
  serial_number<-substr(serial_number,   #isolate serial number
                        gregexpr("SEN.S.N",serial_number)[[1]][1]+9, #Start
                        nchar(serial_number)-1) #stop
  serial_number<-as.numeric(serial_number) 
  
  #Determine TZ
  time_zone<-colnames(temp)[grep("GMT",colnames(temp))]  #Grab collumn name w/ time offset
  time_zone<-substr(time_zone,
                    regexpr('GMT', time_zone)[1],
                    nchar(time_zone))
  time_zone<-if_else(time_zone=="GMT-04:00",
                     "EST",
                     if_else(time_zone=="GMT-05:00",
                             "EDT",
                             "-9999"))
  #Determin units
  units<-colnames(temp)[grep("Abs Pres,",colnames(temp))]
  units<-substr(units,
                regexpr("Abs Pres,", units)+10,
                regexpr("Abs Pres,", units)+12)
  
  #Organize
  colnames(temp)<-c("ID","Timestamp","pressureAbsolute", "temp")
  temp<-temp[,c("Timestamp","pressureAbsolute", "temp")]
  temp<-temp %>%
    #Select collumns of interest
    dplyr::select(Timestamp, pressureAbsolute, temp) %>%
    #Convert to POSIX
    dplyr::mutate(Timestamp = as.POSIXct(strptime(Timestamp, "%m/%d/%y %I:%M:%S %p"), tz = time_zone))  %>%
    #Convert to GMT
    dplyr::mutate(Timestamp = with_tz(Timestamp, "GMT")) %>%
    #Order the intput
    dplyr::arrange(Timestamp)
  
  #create output
  tibble(path       = files[n], 
         Sonde_ID   = serial_number,
         units      = units, 
         start_date = min(temp$Timestamp), 
         end_date   = max(temp$Timestamp))
}

#run function
files<-mclapply(X = seq(1,length(files)), FUN = file_fun, mc.cores = detectCores())
files<-bind_rows(files)

#2.2 Compile well log information---------------------------------------------------
#Create list of file paths
wells<-list.files(working_dir, recursive = T)
wells<-wells[grep(wells,pattern = 'well_log')]
wells<-wells[-grep(wells,pattern = 'archive')]

#Create function to download well log files
log_fun<-function(n){
  
  #Download well log
  temp<-read_csv(paste0(working_dir,wells[n]))
  
  #export temp
  temp
}

#run function
wells<-mclapply(X = seq(1,length(wells)), FUN = log_fun)
wells<-bind_rows(wells)

#Convert times to GMT
wells<-wells %>%
  #Define Timestamp
  mutate(Timestamp = mdy(Date)) %>%
  #Convert to POSIXct and GMT
  mutate(Timestamp = as.POSIXct(Date, format = "%m/%d/%Y", tz = "America/New_York")) %>%
  mutate(Timestamp = with_tz(Timestamp, "GMT")) %>%
  #Add time
  mutate(Timestamp = Timestamp + Time) %>%
  #Idenitfy download date
  mutate(download_date = lubridate::date(Timestamp), 
         download_datetime = Timestamp) %>%
  #Select relevant collumsn
  select(Site_Name, Sonde_ID, download_date, download_datetime, Relative_Water_Level_m)

#Prep files df to join
files <- files %>%
  #estimate download date
  mutate(download_date = date(end_date))

#Correct "download" data where appropratie
files<-files %>%
  #USDA Wells (Lag between handoff)
  mutate(download_date = if_else(download_date == ymd("2017-12-22"), 
                                 ymd("2018-04-09"), 
                                 download_date)) %>%
  #Mikey Likey and Dark Bay [Download error?]
  mutate(download_date = if_else(download_date == ymd("2018-01-04"), 
                                 ymd("2018-02-10"), 
                                 download_date)) %>%
  #Download Error
  mutate(download_date = if_else(download_date == ymd("2017-12-12"), 
                                 ymd("2017-12-20"), 
                                 download_date)) %>%
  #Potential offet issue
  mutate(download_date = if_else(download_date == ymd("2018-03-03"), 
                                 ymd("2018-03-05"), 
                                 download_date)) %>%
  mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                 ymd("2018-03-05"), 
                                 download_date)) %>%
  #Potential offet issue
  mutate(download_date = if_else(download_date == ymd("2018-04-26"), 
                                 ymd("2018-04-28"), 
                                 download_date)) 
  
#join to master lookup table!
wells<-left_join(wells, files) 

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#3.0 Compile Barometric Pressure Logger Info----------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Gather data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Define Baro Loggers
baro_id<-c("10589038", #QB Baro
           "10808360") #GR Baro

#Define Baro Logger Files
baro_files<-wells[wells$Sonde_ID %in% baro_id,] %>% filter(!is.na(path))

#run function
baro<-mclapply(X = paste0(working_dir,baro_files$path), FUN = download_fun, mc.cores = detectCores()) %>% bind_rows()

#Organize barometric pressure
baro<-baro %>%
  #rename baro collumn
  rename(barometricPressure=pressureAbsolute) %>%
  #remove duplicate records from same sonde
  distinct(.) %>%
  #Remove na's
  na.omit()

#Manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#QB Baro Logger
baro_files %>% filter(Site_Name == "QB Baro") 
baro$Timestamp<-if_else(baro$download_date == ymd("2018-01-13"), 
                        baro$Timestamp + hours(5), 
                        baro$Timestamp)
baro$Timestamp<-if_else(baro$download_date == ymd("2018-03-04"), 
                        baro$Timestamp + days(1) + hours(5), 
                        baro$Timestamp)
baro$Timestamp<-if_else(baro$download_date == ymd("2018-04-28"), 
                        baro$Timestamp + hours(5), 
                        baro$Timestamp)
baro$Timestamp<-if_else(baro$download_date == ymd("2018-06-30"),
                        baro$Timestamp + hours(5),
                        baro$Timestamp)
baro$Timestamp<-if_else(baro$download_date == ymd("2018-09-11"),
                        baro$Timestamp + hours(5),
                        baro$Timestamp)
baro$Timestamp<-if_else(baro$download_date == ymd("2018-10-10"), 
                        baro$Timestamp + hours(5), 
                        baro$Timestamp)

#GR Baro Logger
baro_files %>% 
  filter(Site_Name == "GR Baro") %>% 
  select(download_date, download_datetime, end_date) %>% 
  mutate(diff = download_datetime - end_date)
baro$Timestamp<-if_else(baro$Sonde_ID == "10808360" & baro$download_date == ymd("2018-06-24"), 
                        baro$Timestamp - hours(1), 
                        baro$Timestamp) 
baro$Timestamp<-if_else(baro$Sonde_ID == "10808360" & baro$download_date == ymd("2018-09-04"), 
                        baro$Timestamp - hours(1), 
                        baro$Timestamp) 

#Combine datasets and upload!~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Create seperate collumns for each logger
baro<-baro %>%
  select(Timestamp, barometricPressure, Sonde_ID) %>% 
  group_by(Timestamp, Sonde_ID) %>%
  summarise(barometricPressure = mean(barometricPressure)) %>%
  spread(Sonde_ID, barometricPressure)  %>%
  arrange(Timestamp) %>%  
  rename(QB_Baro=`10589038`, GR_Baro=`10808360`) 

#plot with dygraphs
dygraph_ts_fun(baro)

#Combine data from both baro loggers
baro<-baro %>% 
  mutate(Timestamp_15min = ceiling_date(Timestamp, "15 min")) %>% 
  group_by(Timestamp_15min) %>% 
  summarise(QB_Baro = mean(QB_Baro, na.rm=T), 
            GR_Baro = mean(GR_Baro, na.rm=T)) %>%
  mutate(barometricPressure = rowMeans(select(.,QB_Baro, GR_Baro), na.rm=T)) %>%
  rename(Timestamp=Timestamp_15min)

#DB Upload~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Insert barometric pressure data into db
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = baro,
                            method = "baro",
                            site_code = "BARO",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "barometricPressure" = list(column = "barometricPressure", units = "Kilopascal")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' & 
                   ls()!='survey' & 
                   ls()!='wells'])

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#4.0 Estimate Shallow Ground Water Level -------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Read custom R functions
source("functions/download_fun.R")
source("functions/dygraph_ts_fun.R")
source("functions/waterHeight_fun.R")
source("functions/waterDepth_fun.R")
source("functions/baro_fun.R")
source("functions/db_get_ts.R")
source("functions/offset_fun.R")

#Create list of sites
site_names<-unique(wells$Site_Name[grep("Upland",wells$Site_Name)])
site_names<-site_names[order(site_names)]
site_names[grep("Upland", site_names)]

#4.1 BB Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"BB Upland Well 1"
survey_temp<-survey %>% filter(Wetland == "BB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[c(1:5)]<-5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA, 
  wellHeight = survey_temp$`Upland Well Height (m) - Primary`)

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[2:4])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("12-1-2017"))

#Potential disturbance at 7/24/2018(?)

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#4.2 DB Upland Well 1---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"DB Upland Well 1"
survey_temp<-survey %>% filter(Wetland == "DB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff<-rep(5, nrow(well_log))

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA, 
  wellHeight = survey_temp$`Upland Well Height (m) - Primary`)

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[1:4])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("12-1-2017"))

#Potential disturbance at 7/24/2018(?)

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#4.3 DK Upland Well 1---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"DK Upland Well 1"
survey_temp<-survey %>% filter(Wetland == "DK")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[3:7]<-5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date,
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - Primary`)

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(1,2,5,6)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("8-18-2017"))

#Phase shift Oct 6
df<-df %>%
  mutate(waterDepth = if_else(Timestamp<=mdy_hm("10/6/2017 21:15"), 
                              waterDepth + 0.0939, 
                              waterDepth), 
         waterLevel = if_else(Timestamp<=mdy_hm("10/6/2017 21:15"), 
                              waterLevel + 0.0939, 
                              waterLevel)) 

#Potential disturbance at 7/24/2018(?)

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#4.4 DK Upland Well 2---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"DK Upland Well 2"
survey_temp<-survey %>% filter(Wetland == "DK")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Sonde download error on 12/20.  Sonde did not reset. Ran out of battery 1/4.
df<-df %>% 
  mutate(download_date = if_else(download_date == ymd("2018-01-04"), 
                                ymd("2018-02-10"), 
                                download_date))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - 2`)

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(2:6)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("8-18-2017"))

#Phase shift Oct 6
df<-df %>%
  mutate(waterDepth = if_else(Timestamp<=mdy_hm("10/6/2017 20:55"), 
                             waterDepth + 0.2954, 
                             waterDepth), 
         waterLevel = if_else(Timestamp<=mdy_hm("10/6/2017 20:55"), 
                             waterLevel + 0.2954, 
                             waterLevel)) 
#Remove day of death
df<-df %>% filter(Timestamp != mdy("1/3/2018"), 
                  Timestamp != mdy("1/2/2018"))

#Remove NA 
df<-na.omit(df)

#Remove duplicates
df<-df %>% distinct(.)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#4.5 GN Upland Well 1---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"GN Upland Well 1"
survey_temp<-survey %>% filter(Wetland == "GN")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Sonde download error on 12/20.  Sonde did not reset. Ran out of battery 1/4.
df<-df %>% 
  mutate(download_date = if_else(download_date == ymd("2018-01-04"), 
                                 ymd("2018-02-10"), 
                                 download_date))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
# force_diff<-rep(NA, nrow(well_log))
# force_diff[1:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime)#,force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - Primary`)

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[1])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#4.6 GR Upland Well 1---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"GR Upland Well 1"
survey_temp<-survey %>% filter(Wetland == "GR")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Change download date when download died
df<-df %>% 
  mutate(download_date = ifelse(download_date == ymd("2017-12-22"), 
                                ymd("2018-04-09"), 
                                download_date))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
# force_diff<-rep(NA, nrow(well_log))
# force_diff[1:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime)#,force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - Primary`)

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[1:2])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              #"waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.7 GR Upland Well 2---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"GR Upland Well 2"
survey_temp<-survey %>% filter(Wetland == "GR")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Change download date when download died
df<-df %>% 
  mutate(download_date = ifelse(download_date == ymd("2017-12-22"), 
                                ymd("2018-04-09"), 
                                download_date))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
# force_diff<-rep(NA, nrow(well_log))
# force_diff[1:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime)#,force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - 2`)

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[1:2])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              #"waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.8 JB Upland Well 1---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"JB Upland Well 1"
survey_temp<-survey %>% filter(Wetland == "JB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - Primary`)

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(1:5)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("9-20-2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.9 JB Upland Well 2---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"JB Upland Well 2"
survey_temp<-survey %>% filter(Wetland == "JB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - 2`)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(1:6)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("9-20-2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.10 JC Upland Well 1---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"JC Upland Well 1"
survey_temp<-survey %>% filter(Wetland == "JB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - Primary`)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(1:6)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("9-20-2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.11 ND Upland Well 1---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"ND Upland Well 1"
survey_temp<-survey %>% filter(Wetland == "ND")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:2]<- -1
force_diff[3:7]<- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - Primary`)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(1:6)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("9-20-2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.12 ND Upland Well 2---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"ND Upland Well 2"
survey_temp<-survey %>% filter(Wetland == "ND")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:2]<- -1
force_diff[3:7]<- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - 2`)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(1:4)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.13 QB Upland Well 1---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"QB Upland Well 1"
survey_temp<-survey %>% filter(Wetland == "QB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - Primary`)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(2,3,5)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove before deployment
df<-df %>% filter(Timestamp>mdy("9/29/2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.14 QB Upland Well 2---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"QB Upland Well 2"
survey_temp<-survey %>% filter(Wetland == "QB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - 2`)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(1:5)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove before deployment
df<-df %>% filter(Timestamp>mdy("9/30/2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.15 TB Upland Well 1---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"TB Upland Well 1"
survey_temp<-survey %>% filter(Wetland == "TB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1]<- -2
force_diff[2:3]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - Primary`)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(1:2)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove before deployment
df<-df %>% filter(Timestamp>mdy("4/19/2018"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.16 TB Upland Well 2---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"TB Upland Well 2"
survey_temp<-survey %>% filter(Wetland == "TB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1]<- 0
force_diff[2]<- 0
force_diff[3:7] <- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - 2`)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(2:6)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.17 TB Upland Well 3---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"TB Upland Well 3"
survey_temp<-survey %>% filter(Wetland == "TB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1]<- 0
force_diff[2]<- 0
force_diff[3:7] <- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = NA,
  wellHeight = survey_temp$`Upland Well Height (m) - 3`)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(2:6)])

#Water Level [datum = wetland invert]
df$waterLevel = df$waterDepth + depths$offset[depths$event=="survey_upland_well"] 

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove before deployment
df<-df %>% filter(Timestamp>mdy("9/30/2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#4.18 P-26 Deep Well---------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"P-26 Deep Well"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) #%>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:6]<- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = NA, 
  waterDepth = NA,
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(2:4)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove outlier
df<-df %>% filter(waterHeight>1)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
#RSQLite::dbRollback(db)
tf<-Sys.time()
tf-t0

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#5.0 Estimate Wetland Water Level --------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' & 
                   ls()!='survey' & 
                   ls()!='wells'])

#Read custom R functions
source("functions/download_fun.R")
source("functions/dygraph_ts_fun.R")
source("functions/waterHeight_fun.R")
source("functions/waterDepth_fun.R")
source("functions/baro_fun.R")
source("functions/db_get_ts.R")
source("functions/offset_fun.R")

#Create list of sites
site_names<-unique(wells$Site_Name[grep("Wetland",wells$Site_Name)])
site_names<-site_names[order(site_names)]

#5.1 BB Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"BB Wetland Well Shallow"
survey_temp<-survey %>% filter(Wetland == "BB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[c(1:2)]<- 0
force_diff[c(3:8)]<- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[8])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("9-28-2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.2 DB Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"DB Wetland Well Shallow"
survey_temp<-survey %>% filter(Wetland == "DB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:2]<- -1
force_diff[3:8]<- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[8])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("9-28-2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.3 DF Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"DF Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:2]<-  0
force_diff[3:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, 
  waterDepth = NA, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[2:6])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("9-30-2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.4 DK Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"DK Wetland Well Shallow"
survey_temp<-survey %>% filter(Wetland == "DK")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:2]<- -1
force_diff[3:7]<- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[8])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("9-30-2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.5 DV Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"DV Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[3:7]<- 0
force_diff[3:7]<- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, #survey_temp$Date, 
  waterDepth = NA, #survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[2:4])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.6 FN Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"FN Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[3:7]<- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, #survey_temp$Date, 
  waterDepth = NA, #survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(2:6)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove initial rising limb
df<-df %>% filter(Timestamp>mdy("9/29/2017"))

#Deal with shift on Download 12/20 [weird?]
df<-df %>% mutate(waterDepth = if_else(Timestamp<mdy_hm("12/20/2017 19:00"), 
                                       waterDepth - 0.0445, 
                                       waterDepth))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.7 GB Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"GB Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-03"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1]<- -1
force_diff[2:6]<- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, #survey_temp$Date, 
  waterDepth = NA, #survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(1,2,4,5)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove initial rising limb
df<-df %>% filter(Timestamp>mdy("10/1/2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.8 JA Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"JA Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[3:7] <- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, #survey_temp$Date, 
  waterDepth = NA, #survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(1:6)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove initial rising limb
df<-df %>% filter(Timestamp>mdy("9/20/2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.9 JB Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"JB Wetland Well Shallow"
survey_temp<-survey %>% filter(Wetland == "JB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[3:7] <- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(7)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove initial rising limb
df<-df %>% filter(Timestamp>mdy("9/20/2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.10 JC Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"JC Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[3:7] <- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, #survey_temp$Date, 
  waterDepth = NA, #survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(2:6)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove initial rising limb
df<-df %>% filter(Timestamp>mdy("9/20/2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.11 JU Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"JU Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-03"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1]<- 0
force_diff[3:6]<- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, #survey_temp$Date, 
  waterDepth = NA, #survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(3:5)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.12 NB Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"NB Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[3:7] <- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100+100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, #survey_temp$Date, 
  waterDepth = NA, #survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(3:6)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove initial rising limb
df<-df %>% filter(Timestamp>mdy("9/20/2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.13 ND Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"ND Wetland Well Shallow"
survey_temp<-survey %>% filter(Wetland == "ND")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:2]<- -1
force_diff[3:7]<- 5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[8])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("9-30-2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.14 QB Wetland Well Deep--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"QB Wetland Well Deep"
survey_temp<-survey %>% filter(Wetland == "QB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:7] <- -1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100+100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(7)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove initial rising limb
df<-df %>% filter(Timestamp>mdy("9/30/2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.15 QB Wetland Well Shallow-------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"QB Wetland Well Shallow"
survey_temp<-survey %>% filter(Wetland == "QB")

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Deal with April 26th download
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-04-26"), 
                                          ymd("2018-04-28"), 
                                          download_date))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:6]<-5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100+100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = survey_temp$Date, 
  waterDepth = survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(6)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove initial rising limb
df<-df %>% filter(Timestamp>mdy("12/1/2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.16 SB Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"SB Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:2]<-0
force_diff[3]<--1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100+100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, 
  waterDepth = NA, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[2])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Remove pre-deployment 
df<-df %>% filter(Timestamp>mdy("9-30-2017"))

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.17 TA Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"TA Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:2]<-0
force_diff[3:7]<-5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, #survey_temp$Date, 
  waterDepth = NA, #survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(2,3,5)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.18 TB Wetland Well Shallow--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"TB Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:2]<-0
force_diff[3:8]<-5

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA, #survey_temp$Date, 
  waterDepth = NA, #survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(2:7)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.19 TI Wetland Well Shallow-------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"TI Wetland Well Shallow"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"), 
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df, 
               download = ymd("2018-03-05"), 
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]), 
               offset = hours(24))

#Deal with April 26th download
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-04-26"), 
                                          ymd("2018-04-28"), 
                                          download_date))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff[1:2]<-0
force_diff[3:7]<--1

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp, 
                    pressureAbsolute = df$pressureAbsolute, 
                    barometricPressure = df$barometricPressure, 
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100+100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m, 
  #from survey file
  surveyDate = NA,#survey_temp$Date, 
  waterDepth = NA,#survey_temp$`Water Depth (m)`, 
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[c(3:6)])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA 
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#5.20 GN and GR Wetland Wells Shallow-------------------------------------------------------
#GN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
t0<-Sys.time()
df<-readxl::read_xlsx(paste0(working_dir,"USDA_JacksonLane/Data_JacksonLane.xlsx")) %>%
    rename(Timestamp = Date, waterDepth = JNAT_sw) %>% select(Timestamp, waterDepth) %>% na.omit()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "GN Wetland Well Shallow",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter")))
tf<-Sys.time()
tf-t0

#GR ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
t0<-Sys.time()
df<-readxl::read_xlsx(paste0(working_dir,"USDA_JacksonLane/Data_JacksonLane.xlsx")) %>%
  rename(Timestamp = Date, waterDepth = JRES_sw) %>% select(Timestamp, waterDepth) %>% na.omit()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "GR Wetland Well Shallow",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "waterLevel" = list(column = "waterDepth", units = "Meter")))
tf<-Sys.time()
tf-t0

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#6.0 Estimate Catchment Outlet Water Levels-----------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='survey' &
                   ls()!='wells'])

#Read custom R functions
source("functions/download_fun.R")
source("functions/dygraph_ts_fun.R")
source("functions/waterHeight_fun.R")
source("functions/waterDepth_fun.R")
source("functions/baro_fun.R")
source("functions/db_get_ts.R")
source("functions/offset_fun.R")

#Create list of sites
site_names<-unique(wells$Site_Name[grep("Catchment",wells$Site_Name)])
site_names<-site_names[order(site_names)]
# 
#              
# # [5] "Jones Road North Catchment Outlet" "Jones Road South Catchment Outlet"
# # [7] "Mikey Likey Catchment Outlet"      "Solute Catchment Outlet"          
# # [9] "Tiger Paw Catchment Outlet"        "Tracer Catchment Outlet" 
# 
#6.1 Denver Catchment Outlet--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"Denver Catchment Outlet"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows()

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"),
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df,
               download = ymd("2018-03-05"),
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]),
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff<-c(0,0,-1,-1,-1,-1,-1)

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp,
                    pressureAbsolute = df$pressureAbsolute,
                    barometricPressure = df$barometricPressure,
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date,
                    end_date = well_log$end_date,
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>%
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = NA, #survey_temp$Date,
  waterDepth = NA, #survey_temp$`Water Depth (m)`,
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[3:5])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>%
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#6.2 Greg Catchment Outlet----------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"Greg Catchment Outlet"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows()

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff<-c(-1, 5, 5, 5)

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp,
                    pressureAbsolute = df$pressureAbsolute,
                    barometricPressure = df$barometricPressure,
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date,
                    end_date = well_log$end_date,
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>%
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = NA, #survey_temp$Date,
  waterDepth = NA, #survey_temp$`Water Depth (m)`,
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[1:2])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>%
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#6.3 Dogbone Catchment Outlet-------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"Dogbone Catchment Outlet"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows()

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
#force_diff<-c(-1, 5, 5, 5)

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp,
                    pressureAbsolute = df$pressureAbsolute,
                    barometricPressure = df$barometricPressure,
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date,
                    end_date = well_log$end_date,
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>%
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = NA, #survey_temp$Date,
  waterDepth = NA, #survey_temp$`Water Depth (m)`,
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[1:2])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>%
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#6.4 Jones Road North Catchment Outlet-------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"Jones Road North Catchment Outlet"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows()

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"),
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df,
               download = ymd("2018-03-05"),
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]),
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff<-c(0, 0, -1, -1,-1,-1,-1)

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp,
                    pressureAbsolute = df$pressureAbsolute,
                    barometricPressure = df$barometricPressure,
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date,
                    end_date = well_log$end_date,
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>%
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = NA, #survey_temp$Date,
  waterDepth = NA, #survey_temp$`Water Depth (m)`,
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[2:5])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>%
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#6.5 Jones Road South Catchment Outlet-------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"Jones Road South Catchment Outlet"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows()

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"),
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df,
               download = ymd("2018-03-05"),
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]),
               offset = hours(24))

#Change download date for december 12
df<-df %>% 
  mutate(download_date = if_else(download_date==ymd("2017-12-12"), 
                                 ymd("2017-12-20"), 
                                 download_date))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff<-c(0, 24,-19,5,5,5)

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp,
                    pressureAbsolute = df$pressureAbsolute,
                    barometricPressure = df$barometricPressure,
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date,
                    end_date = well_log$end_date,
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>%
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = NA, #survey_temp$Date,
  waterDepth = NA, #survey_temp$`Water Depth (m)`,
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[2:5])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>%
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#6.6 Mikey Likey Catchment Outlet-------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"Mikey Likey Catchment Outlet"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows()

#Change download date for jan 04
df<-df %>% 
  mutate(download_date = if_else(download_date==ymd("2018-01-04"), 
                                 ymd("2018-02-10"), 
                                 download_date))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff<-c(-1,-1,-1,-1,-1)

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp,
                    pressureAbsolute = df$pressureAbsolute,
                    barometricPressure = df$barometricPressure,
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date,
                    end_date = well_log$end_date,
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>%
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = NA, #survey_temp$Date,
  waterDepth = NA, #survey_temp$`Water Depth (m)`,
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[1:2])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>%
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#6.7 Solute Catchment Outlet--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"Solute Catchment Outlet"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows()

#Offset March download by atleast 1 day
df<-df %>% mutate(download_date = if_else(download_date == ymd("2018-03-04"),
                                          ymd("2018-03-05"),
                                          download_date))
df<-offset_fun(df,
               download = ymd("2018-03-05"),
               path = paste0(working_dir,well_log$path[well_log$download_date=="2018-03-05"]),
               offset = hours(24))

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))
force_diff<-c(0,0,-1,-1,-1,-1)

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp,
                    pressureAbsolute = df$pressureAbsolute,
                    barometricPressure = df$barometricPressure,
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date,
                    end_date = well_log$end_date,
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>%
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = NA, #survey_temp$Date,
  waterDepth = NA, #survey_temp$`Water Depth (m)`,
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[1:4])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>%
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#6.8 Tiger Paw Catchment Outlet--------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"Tiger Paw Catchment Outlet"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows()

#Plot for funzies
dygraph_ts_fun(df %>% select(Timestamp, pressureAbsolute))

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate barometric pressure
df$barometricPressure<-baro_fun(df$Timestamp, db, 'BARO')

#Define minor offsets
force_diff<-rep(NA, nrow(well_log))

#Estimate water depth
df<-waterHeight_fun(Timestamp = df$Timestamp,
                    pressureAbsolute = df$pressureAbsolute,
                    barometricPressure = df$barometricPressure,
                    temp = df$temp,
                    download_date_ts = df$download_date,
                    download_date_log = well_log$download_date,
                    start_date = well_log$start_date,
                    end_date = well_log$end_date,
                    download_datetime = well_log$download_datetime,
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>%
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and water level
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m,
  #from survey file
  surveyDate = NA, #survey_temp$Date,
  waterDepth = NA, #survey_temp$`Water Depth (m)`,
  wellHeight = NA)
depths

#Water depth
df$waterDepth = df$waterHeight + mean(depths$offset[3:5])

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove NA
df<-na.omit(df)

#Plot for funzies
dygraph_ts_fun(df %>%
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = site,
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0
