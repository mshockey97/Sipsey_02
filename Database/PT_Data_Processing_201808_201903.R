#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name:  PT Data Processing
# Coder: C. Nathan Jones
# Date: 3 July 2019
# Purpose: Process PT Data collected across the Tully Lab SWI sites
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#GR results are odd. Need to fix Baro file and Karen G's sondes

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#1.0 Setup Worskspace-----------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
source("R/download_fun.R")
source("R/dygraph_ts_fun.R")
source("R/waterHeight_fun.R")
source("R/waterDepth_fun.R")
source("R/baro_fun.R")
source("R/db_get_ts.R")
source("R/offset_fun.R")

#Define working dir
working_dir<-"//nfs/njones-data/Research Projects/SWI/PT_Data/"

#Set system time zone 
Sys.setenv(TZ="America/New_York")

#Define database connection
db<-dbConnect(RSQLite::SQLite(),paste0(working_dir,'SWI.sqlite'))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#2.0 Create lookup table for PT data files--------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#2.1 Compile a list of file paths-----------------------------------------------
#Identify files with HOBO export files
files<-list.files(working_dir, recursive = T)
files<-files[substr(files,nchar(files)-2,nchar(files))=="csv"] 
files<-files[grep(files,pattern = "export")]

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

#2.2 Compile well log information-----------------------------------------------
#Create list of file paths
wells<-list.files(working_dir, recursive = T)
wells<-wells[grep(wells,pattern = 'well_log')]


#Create function to download well log files
log_fun<-function(n){
  
  #Download well log
  temp<-read_csv(paste0(working_dir,wells[n]))
  
  #export temp
  temp
}

#run function
wells<-mclapply(X = seq(1,length(wells)), FUN = log_fun)
wells<-bind_rows(wells) %>% select(Site_Name, Date, Time, Sonde_ID, Relative_Water_Level_m, Notes)

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

#2.3 Manual checks--------------------------------------------------------------
#Check if SN in log files match SN in download files
files[!(files$Sonde_ID %in% wells$Sonde_ID),]
wells[!(wells$Sonde_ID) %in% files$Sonde_ID,]

#2.4 Merge wells---------------------------------------------------------------
#Prep files df to join
files <- files %>%
  #estimate download date
  mutate(download_date = paste(month(end_date)))

#join to master lookup table!
wells<-wells %>%
  mutate(download_date = paste(month(download_date))) %>%
  left_join(., files) 

#Cleanup workspace
remove(files);remove(file_fun);remove(log_fun)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#3.0 Compile Barometric Pressure Logger Info------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#3.1 Gather data----------------------------------------------------------------
#Define Baro Logger Files
baro_files<-wells %>% filter(str_detect(Site_Name, 'BN-Baro'))

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

#plot with dygraphs
dygraph_ts_fun(baro %>% select(Timestamp,barometricPressure))

#3.2 DB Upload------------------------------------------------------------------
#Insert barometric pressure data into db
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


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#4.0 Estimate Shallow Ground Water Level ---------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
site_names<-wells %>% select(Site_Name) %>% arrange(Site_Name)
c(site_names)
# "AL-A"    "AL-B"    "AL-C"    "AL-D"    "BN-A"    "BN-A"    "BN-B"    "BN-B"    "BN-Baro" "BN-C"    "BN-C"    "BN-D"   
# [13] "BN-E"    "EL-A"    "EL-B"    "EL-C"    "EP-A"    "EP-B"    "EP-C"    "GR-A"    "GR-A"    "GR-B"    "GR-B"    "GR-Baro"
# [25] "GR-C"    "GR-C"    "GR-D"    "GR-E"    "GR-F" 

#4.1 AL-A-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"AL-A"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
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
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+depths$offset


#Plot and manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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

#4.2 AL-B-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"AL-B"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
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
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+depths$offset

#Plot and manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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

#4.3 AL-C-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"AL-C"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
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
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+depths$offset

#Plot and manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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

#4.4 AL-D-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"AL-D"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) %>% na.omit()

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
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
  download_date = well_log$download_date, Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+depths$offset

#Plot and manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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

#4.5 BN-A-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"BN-A"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
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
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot and manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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

#4.6 BN-B-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"BN-B"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
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
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot and manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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

#4.6 BN-C-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"BN-C"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
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
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot and manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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


#4.7 BN-D-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"BN-D"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
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
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot and manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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

#4.8 BN-E-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"BN-E"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
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
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot and manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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

#4.9 EP-A-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"EP-A"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and make manual edits as needed~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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

#4.10 EP-B-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"EP-B"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and make manual edits as needed~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

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

#4.11 EP-C-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"EP-C"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and make manual edits as needed~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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

#4.12 GR-A-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"GR-A"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and make manual edits as needed~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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

#4.13 GR-B-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"GR-B"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and make manual edits as needed~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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

#4.14 GR-C-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"GR-C"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and make manual edits as needed~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
#4.15 GR-D-----------------------------------------------------------------------
#Organize Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify site and survey data
site<-"GR-D"

#Identify well info
well_log<-wells %>% filter(Site_Name==site) 

#Download pressure data
df<-mclapply(paste0(working_dir,well_log$path), download_fun) %>% bind_rows() 

#Make Depth Calculations~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
                    download_date_log = date(well_log$end_date),
                    start_date = well_log$start_date, 
                    end_date = well_log$end_date, 
                    download_datetime = well_log$download_datetime, 
                    force_diff = force_diff)

#Examine waterHeight_fun output [iterate if needed using force_diff or offset_fun]
h_report
dygraph_ts_fun(df %>% 
                 mutate(waterHeight=waterHeight*100) %>%
                 select(Timestamp, waterHeight, pressureAbsolute, barometricPressure))

#Estimate water depth and make manual edits as needed~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate water depth and water level 
depths<-waterDepth_fun(
  #from working df
  Timestamp = df$Timestamp, waterHeight = df$waterHeight,
  #from well log
  download_date = date(well_log$end_date), Relative_Water_Level_m = well_log$Relative_Water_Level_m)
df$waterDepth<-df$waterHeight+mean(depths$offset, na.rm=T)

#Plot for funzies
dygraph_ts_fun(df %>% 
                 mutate(waterDepth=waterDepth*100+1000) %>%
                 select(Timestamp, waterDepth))

#Remove NA 
df<-na.omit(df)

#Insert into database~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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

#5.0 Disconnect from DB---------------------------------------------------------
dbDisconnect(db)
