#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name: Initial PT Data Processing
# Coder: C. Nathan Jones
# Date: 16 April 2019
# Purpose: Process PT Data collected across the Palmer Lab Delmarva wetland sites
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#Things to do:
  #1: Deal with differences between downloads. 
  #2: Ask greg for missing 2017 data...
  #3: Spot all data for random outliers
  #4: Look at weird jump Jan 12 in DF and QB
  #5: Look at PT change in October for all DK sites
  #6: Why did ND Upland 1 logger change? Check this for all sites. FML

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1: Setup Worskspace ---------------------------------------------------------
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

#Define working dir
working_dir<-"//nfs/palmer-group-data/Choptank/Nate/PT_Data/"

#Set system time zone 
Sys.setenv(TZ="America/New_York")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2: Setup Database -----------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#2.1 Create dabase~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Create SQLight databse (only do this once)
db <- create_sqlite(dir = working_dir, filename = "choptank", connect = T)

#2.2 Insert equipment iunformation~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Load existing equipment table
equipment<-read.csv(paste0(working_dir,"/Database Information/equipment.csv"))

#Create wrapper function for the db_describe_equipment
fun<-function(n){db_describe_equipment(db, 
                                       equip_name    =   as.character(equipment$serial_no[n]), 
                                       serial_no     =   equipment$serial_no[n],
                                       model_name    =   "U20 Pressure Transducer",
                                       vendor        =   "Onset",
                                       manufacturer  =   "HOBO",
                                       equipment_type=   "pressureTransducer",
                                       owner_first   =   "Margaret",
                                       owner_last    =   "Palmer",
                                       owner_email   =   "mpalmer@sesync.org")}

#intiate function
lapply(seq(1, length(equipment[,1])), fun)

#2.3 Insert information about sites~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Load existing information about sites
sites<-read.csv(paste0(working_dir,"/Database Information/sites.csv"))

#Create wrapper function for the db_describe_site function
fun<-function(n){db_describe_site(db, site_code = sites$site_code[n])}

#Inititate function
lapply(seq(1, length(sites[,1])), fun)

#2.4 Insert information about methods~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
db_describe_method(db, 
                   methodname = "pt", 
                   methodcode = "pt",
                   methodtypecv = "Instrument deployment",
                   methoddescription = "pressure transducer")

db_describe_method(db, 
                   methodname = "baro", 
                   methodcode = "baro",
                   methodtypecv = "Instrument deployment",
                   methoddescription = "barometric logger")

db_describe_method(db, methodname = "waterdepth", methodcode = "waterdepth",
                   methodtypecv = "Derivation",
                   methoddescription = "Calculate water depth from baro and pt")

#2.5 Describe variables~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Barometric Pressure (from baro logger)
db_describe_variable(db, 
                     variabletypecv = "Hydrology",
                     variablecode   = "barometricPressure",
                     variablenamecv = "barometricPressure")

#Absolute Pressure
db_describe_variable(db, 
                     variabletypecv = "Hydrology",
                     variablecode   = "pressureAbsolute",
                     variablenamecv = "pressureAbsolute")

#Gage Pressure
db_describe_variable(db, 
                     variabletypecv = "Hydrology",
                     variablecode   = "pressureGauge",
                     variablenamecv = "pressureGauge")

#Water Collumn height [no datum] 
db_describe_variable(db, 
                     variabletypecv = "Hydrology",
                     variablecode   = "waterColumnEquivalentHeightAbsolute",
                     variablenamecv = "waterColumnEquivalentHeightAbsolute")

#Water Collumn Depth 
db_describe_variable(db, 
                     variabletypecv = "Hydrology",
                     variablecode   = "gageHeight",
                     variablenamecv = "gageHeight")

#Offset (to correct water level to water depth)
db_describe_variable(db, 
                     variabletypecv = "Hydrology",
                     variablecode   = "offset",
                     variablenamecv = "offset")

#Water Depth (ground surface = 0, positive values indicate inundation)
db_describe_variable(db, 
                     variabletypecv = "Hydrology",
                     variablecode   = "waterLevel",
                     variablenamecv = "waterLevel")

#Code to interact with the db 
#db<-dbConnect(RSQLite::SQLite(),"//nfs/palmer-group-data/Choptank/Nate/PT_Data/choptank.sqlite")
#dbDisconect()
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3: Create file lookup table--------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Identify files with downloads
files<-list.files(working_dir, recursive = T)
files<-files[substr(files,nchar(files)-2,nchar(files))=="csv"] 

#Remove non-relevant files 
files<-files[-grep(files,pattern = 'archive')]
files<-files[-grep(files,pattern = 'Database Information')]
files<-files[-grep(files,pattern = 'intermediate')]
files<-files[-grep(files,pattern = 'precip')]

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
  
  #Organize
  colnames(temp)<-c("ID","Timestamp","pressureAbsolute", "temp")
  temp<-temp[,c("Timestamp","pressureAbsolute", "temp")]
  temp<-temp %>% 
    dplyr::select(Timestamp, pressureAbsolute, temp) %>%
    dplyr::mutate(Timestamp = as.POSIXct(strptime(Timestamp, "%m/%d/%y %I:%M:%S %p"))) %>%
    dplyr::arrange(Timestamp) 
  
  #create output
  tibble(path       = files[n], 
         pt_id      = serial_number,
         start_date = min(temp$Timestamp), 
         end_date   = max(temp$Timestamp))
}

#run function
files<-mclapply(X = seq(1,length(files)), FUN = file_fun)
files<-bind_rows(files)

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files'])

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4: Estimate barometric pressure----------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Gather data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Define Baro Loggers
baro_id<-c("10589038",  #JR Baro
           "10808360") #JL Baro

#Define Baro Logger Files
baro_files<-files[files$pt_id %in% baro_id,]

#Download all files, then only keep files needed
baro_fun<-function(n){
  
  #Download data
  temp<-read_csv(paste0(working_dir,baro_files$path[n]), skip=1)
  
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
  colnames(temp)<-c("ID","Timestamp","barometricPressure", "temp")
  temp<-temp[,c("Timestamp","barometricPressure", "temp")]
  temp<-temp %>% 
    #Select collumns of interest
    dplyr::select(Timestamp, barometricPressure, temp) %>%
    #Convert to POSIX
    dplyr::mutate(Timestamp = as.POSIXct(strptime(Timestamp, "%m/%d/%y %I:%M:%S %p"), tz = time_zone)) %>%
    #Convert to GMT time
    dplyr::mutate(Timestamp = with_tz(Timestamp, "GMT")) %>%
    #Order the intput
    dplyr::arrange(Timestamp) 
  
  #Convert from psi to kpa if necessary
  if(units=="psi"){temp$barometricPressure<-6.89476*temp$barometricPressure}
  
  #Add serial number
  temp$pt_id<-baro_files$pt_id[n]
  
  #Add download data
  temp$download_date<-date(baro_files$end_date[n])
  
  #Export temp
  temp
}
  
#run function
baro<-mclapply(X = seq(1,nrow(baro_files)), FUN = baro_fun)
baro<-bind_rows(baro)

#Organize barometric pressure
baro<-baro %>%
  #remove duplicate records from same sonde
  distinct(.) %>%
  #Remove na's
  na.omit()

#Manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#This file was one day backwards [weird]
baro$Timestamp<-if_else(baro$download_date=="2018-03-04", 
                        baro$Timestamp+days(1), 
                        baro$Timestamp)

#Correct Baro from Greg [seems off, likely a TZ issue]
baro$Timestamp<-if_else(baro$pt_id=="10808360" & baro$Timestamp<mdy("12/22/2017"),
                        baro$Timestamp - hours(5), 
                        baro$Timestamp)

#Correct Sonde again. [Apprarently the computer UTC TZ does not directly apply to sonde tz, its just a label]
baro$Timestamp<-if_else(baro$pt_id=="10808360" & baro$Timestamp>mdy("12/22/2017"),
                        baro$Timestamp - hours(6), 
                        baro$Timestamp)

#Take moving avarege [15 minute]
baro<-baro %>% 
  mutate(Timestamp = ceiling_date(Timestamp, "5 min")) %>%
  group_by(Timestamp) %>%
  summarise(barometricPressure = mean(barometricPressure, na.rm = T), 
            temp = mean(temp, na.rm = T))

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
                              "barometricPressure" = list(column = "barometricPressure", units = "Kilopascal"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                 ls()!='db' &
                 ls()!='files'])

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 5: Insert time series---------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#5.1 Create well lookup table-------------------------------------------------------
#Create list of files
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
  #Convert to POSIXct
  mutate(Timestamp = as.POSIXct(Date, format = "%m/%d/%Y", tz = "America/New_York")) %>%
  #Add time
  mutate(Timestamp = Timestamp + Time) %>%
  #Idenitfy download date
  mutate(download_date = lubridate::date(Timestamp)) %>%
  #Select relevant collumsn
  select(Site_Name, Sonde_ID, Timestamp, download_date, Relative_Water_Level_m)

#Prep files df to join
files <- files %>%
  #rename pt_id
  rename(Sonde_ID = pt_id) %>%
  #estimate download date
  mutate(download_date = date(end_date))

#Correct "download" data where appropratie
files<-files %>%
  mutate(download_date = if_else(download_date == mdy("6/23/2018"), 
                                 mdy("6/24/2018"), 
                                 download_date)) %>%
  mutate(download_date = if_else(download_date == mdy("10/10/2018"), 
                                 mdy("10/11/2018"), 
                                 download_date)) %>%
  mutate(download_date = if_else(download_date == mdy("4/26/2018"), 
                                 mdy("4/28/2018"), 
                                 download_date)) %>%
  mutate(download_date = if_else(download_date == mdy("3/4/2018"), 
                                 mdy("3/5/2018"), 
                                 download_date)) %>%
  mutate(download_date = if_else(download_date == mdy("10/11/2018"), 
                                 mdy("10/10/2018"), 
                                 download_date)) 
  
#join to master lookup table!
wells<-left_join(wells, files, by = c("download_date" = "download_date", "Sonde_ID" = "Sonde_ID"))

#5.2 Downlaod well data-------------------------------------------------------------
#Download functions
source("functions/db_get_ts.R")
source("functions/waterHeight_fun.R")
source("functions/waterDepth_fun.R")
source("functions/dygraph_ts_fun.R")

#Identify unique sites
sites<-unique(wells$Site_Name)

#Download survey data
survey<-read_csv(paste0(working_dir,"survey_data/survey_V1.csv"))

#Order of opperations for indiviudal sonde data
  #Download files and estiamte height using waterHeight_fun
  #Conduct manual edits on water level data
  #Estimate water depth using waterDepth_fun
  #Estimate water level using waterLevel_fun [if upland well]
  #Upload to db

#5.3 Process wetland water level data-----------------------------------------------


#BB Wetland Well Shallow------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("BB Wetland Well Shallow", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Adjust tiem before Nov 30
df$pressureAbsolute<-if_else(df$Timestamp< mdy("11/30/2017") &
                               df$Timestamp> mdy("10/20/2017"), 
                             lead(df$pressureAbsolute, n=60), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972
df<-df %>% filter(Timestamp<mdy_h("11/30/2017 9") | Timestamp>mdy_h("11/30/2017 16"))

#Adjust tiem before Oct 20
df$pressureAbsolute<-if_else(df$Timestamp<= mdy("10/20/2017"),
                             lead(df$pressureAbsolute, n=24), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972
df<-df %>% filter(Timestamp<mdy_h("11/30/2017 9") | Timestamp>mdy_h("11/30/2017 16"))

#Remove 
df<-df %>% filter(Timestamp> mdy("9/28/2017"))

#Estimate Water Depth~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("BB Wetland Well Shallow", df, wells, survey) %>%
  filter(measurement == "Water Depth (m)") %>%
  select(diff)

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        barometricPressure,
                        pressureAbsolute,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+100))

#Insert into the BB~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "BB Wetland Well Shallow",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#DB Wetland Well Shallow------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("DB Wetland Well Shallow", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove outlier in January 2018
df<-df %>% filter(waterColumnEquivalentHeightAbsolute<2)

#Adjust tiem before Nov 30
df$pressureAbsolute<-if_else(df$Timestamp< mdy("11/30/2017"), 
                             lead(df$pressureAbsolute, n=60), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972
df<-df %>% filter(Timestamp<mdy_h("11/30/2017 9") | Timestamp>mdy_h("11/30/2017 16"))

#Estimate Water Level~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("DB Wetland Well Shallow", df, wells, survey) %>%
  filter(measurement == "Water Depth (m)") %>%
  select(diff)

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        temp,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+100))

#Insert into the db~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "DB Wetland Well Shallow",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#DF Wetland Well Shallow------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("DF Wetland Well Shallow", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove firt three days [before PT deployment]
df<-df %>% filter(Timestamp > mdy("9/30/2017"))

#Adjust tiem before Nov 30
df$pressureAbsolute<-if_else(df$Timestamp< mdy("11/30/2017"), 
                             lead(df$pressureAbsolute, n=60), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972
df<-df %>% filter(Timestamp<mdy_h("11/30/2017 9") | Timestamp>mdy_h("11/30/2017 16"))

#Adjust tiem between Nov 30 and Jan 13
df$pressureAbsolute<-if_else(df$Timestamp > mdy("11/30/2017") &
                             df$Timestamp < mdy("1/13/2018"),
                             lead(df$pressureAbsolute, n=72), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972


#Adjust time between Jan 13 and March 4
df$pressureAbsolute<-if_else(df$Timestamp > mdy("1/13/2018") &
                               df$Timestamp < mdy("3/4/2018"),
                             lag(df$pressureAbsolute, n=216), #Add 6 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972
df<-df %>% filter(Timestamp<mdy("1/12/2018") | Timestamp>mdy("1/15/2018"))

#Adjust time between March 4 and April 28
df$pressureAbsolute<-if_else(df$Timestamp > mdy("3/4/2018") &
                               df$Timestamp < mdy("4/29/2018"),
                             lead(df$pressureAbsolute, n=70), #Add 6 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#Adjust time between April 28 and June 5
df$pressureAbsolute<-if_else(df$Timestamp > mdy("4/29/2018") &
                               df$Timestamp < mdy("6/30/2018"),
                             lead(df$pressureAbsolute, n=67), #Add 6 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#Adjust time between April 28 and June 5
df$pressureAbsolute<-if_else(df$Timestamp > mdy("6/30/2018") &
                               df$Timestamp < mdy("9/11/2018"),
                             lead(df$pressureAbsolute, n=76), #Add 6 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#Adjust time between April 28 and June 5
df$pressureAbsolute<-if_else(df$Timestamp > mdy("9/11/2018") &
                               df$Timestamp < mdy("10/10/2018"),
                             lead(df$pressureAbsolute, n=69), #Add 6 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#Estimate Water Level~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("DF Wetland Well Shallow", df, wells, survey) %>%
  na.omit() %>%
  filter(diff > -1) %>%
  group_by(measurement) %>% summarise(diff = mean(diff))

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#remove na's
df<-na.omit(df)

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        temp,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+100))

#Insert into the db~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "DF Wetland Well Shallow",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#DK Wetland Well Shallow------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("DK Wetland Well Shallow", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Offset pressure before Nov 30 by 5 hours
df$pressureAbsolute<-if_else(df$Timestamp< mdy("11/30/2017"), 
                             lead(df$pressureAbsolute, n=60), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#Adjust for Octoaber 6 re-installtation
t_event<-mdy_hm("10/6/2017 17:00")
y_before<-mean(df$waterColumnEquivalentHeightAbsolute[df$Timestamp<t_event & df$Timestamp>(t_event-hours(1))])
y_after<-mean(df$waterColumnEquivalentHeightAbsolute[df$Timestamp>(t_event+minutes(10)) & df$Timestamp<(t_event+hours(1))])
diff<-y_after-y_before
df$waterColumnEquivalentHeightAbsolute[df$Timestamp<=t_event] <- df$waterColumnEquivalentHeightAbsolute[df$Timestamp<=t_event] + diff
df<-df[!(df$Timestamp==t_event),]
df<-df[!(df$Timestamp==(t_event-minutes(5))),]

#Estimate Water Level~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("DK Wetland Well Shallow", df, wells, survey) %>%
  filter(measurement == "Water Depth (m)") %>%
  select(diff)

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        temp,
                        waterColumnEquivalentHeightAbsolute) %>%
                 mutate(waterColumnEquivalentHeightAbsolute = waterColumnEquivalentHeightAbsolute*100))

#Insert into the db~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "DK Wetland Well Shallow",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#GN Wetland Well Shallow------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~~~~~~~~~~~~~~~~~~~ USDA-ARS maintains this wells ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Download data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#download data
df<-readxl::read_xlsx(paste0(working_dir,"USDA_JacksonLane/Data_JacksonLane.xlsx"))

#Gap fill data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Organize
model<-df %>% 
  #Select wetlands of interest
  select(Date, JRES_sw, JNAT_sw) %>%
  #rename collumns
  rename(Timestamp = Date,
         nat       = JNAT_sw,
         res       = JRES_sw) %>%
  #Remove NAs
  na.omit()

#Create polynomial regression for gap filling
model<-lm(nat ~ poly(res,5), data=model)

#Project model onto missing data
df<-df %>% 
  #Select wetlands of interest
  select(Date, JRES_sw, JNAT_sw) %>%
  #rename collumns
  rename(Timestamp = Date,
         nat       = JNAT_sw,
         res       = JRES_sw) 
df$predicted<-predict(model, data.frame(res = df$res))

#Plot
plot(df$res, df$nat, pch=19, col="grey30", cex=.3)
points(df$res, df$predicted, pch=19, col="red", cex=0.3)

#Gap fill
df<-df %>% 
  #Gap fill
  mutate(nat = if_else(is.na(nat), 
                       predicted,
                       nat)) %>%
  #Organize Columns
  rename(waterDepth = nat) %>%
  select(Timestamp, waterDepth) %>%
  #Force TZ
  mutate(Timestamp = force_tz(Timestamp, "America/New_York")) %>%
  #Remove NA
  na.omit()

#plot for funzies
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+100))

#Export to databse~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "GN Wetland Well Shallow",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#ND Wetland Well Shallow------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("ND Wetland Well Shallow", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove period before installation
df<-df %>% filter(Timestamp> mdy("9/29/2017"))

#Kill dat crazy ass outlier
df<-df %>% filter(waterColumnEquivalentHeightAbsolute<1.9)

#Adjust tiem before Nov 30
df$pressureAbsolute<-if_else(df$Timestamp< mdy("11/30/2017") &
                               df$Timestamp> mdy("10/20/2017"), 
                             lead(df$pressureAbsolute, n=60), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#Adjust tiem before Oct 20
df$pressureAbsolute<-if_else(df$Timestamp<= mdy("10/20/2017"),
                             lead(df$pressureAbsolute, n=48), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#Kill funkiness
df<-df %>% filter(floor_date(Timestamp, unit="day")!=mdy("11/29/17"))

#Estimate Water Level~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("ND Wetland Well Shallow", df, wells, survey) %>%
  filter(measurement == "Water Depth (m)") %>%
  select(diff)

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        barometricPressure,
                        pressureAbsolute,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+100))

#Insert into the db~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "ND Wetland Well Shallow",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#TB Wetland Well Shallow------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("TB Wetland Well Shallow", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Adjust tiem before Nov 30
df$pressureAbsolute<-if_else(df$Timestamp< mdy("11/30/2017") &
                               df$Timestamp> mdy("10/20/2017"), 
                             lead(df$pressureAbsolute, n=60), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972
df<-df %>% filter(Timestamp<mdy_h("11/30/2017 9") | Timestamp>mdy_h("11/30/2017 16"))

#Adjust tiem before Oct 20
df$pressureAbsolute<-if_else(df$Timestamp<= mdy("10/20/2017"),
                             lead(df$pressureAbsolute, n=60), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#Estimate Water Level~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("TB Wetland Well Shallow", df, wells, survey) %>%
  filter(measurement == "Water Depth (m)") %>%
  select(diff)

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        barometricPressure,
                        pressureAbsolute,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+100))

#Insert into the db~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "TB Wetland Well Shallow",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#QB Wetland Well Shallow------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("QB Wetland Well Shallow", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#Remove rando outlier
df<- df %>% filter(pressureAbsolute<125)

#Adjust time between 1/13 and 4/28
df$pressureAbsolute<-if_else(df$Timestamp> mdy("1/12/2018") &
                               df$Timestamp< mdy("3/3/2018"), 
                             lag(df$pressureAbsolute, n=288), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#Remove odd gaps
df<-df %>% filter(Timestamp>mdy("12/1/2017"))
df<-df %>% filter(Timestamp<mdy("1/12/2018") | Timestamp>mdy("1/15/2018"))
df<-df %>% filter(Timestamp<mdy("3/2/2018") | Timestamp>mdy("3/6/2018"))

#Gap fill before December wtih DF
gap<-db_get_ts(db, "DF Wetland Well Shallow", "waterDepth", mdy("10/1/2017"),mdy("11/30/2017"))
diff<-mean(gap$waterDepth[(nrow(gap)-12):nrow(gap)]) - mean(df$waterColumnEquivalentHeightAbsolute[1:10])
gap <-gap %>% 
  mutate(waterColumnEquivalentHeightAbsolute = waterDepth-diff) %>% 
  select(Timestamp, waterColumnEquivalentHeightAbsolute)
df<-df %>% 
  select(Timestamp, waterColumnEquivalentHeightAbsolute) %>%
  bind_rows(df, gap) %>%
  arrange(Timestamp)

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        waterColumnEquivalentHeightAbsolute) %>%
                 mutate(waterColumnEquivalentHeightAbsolute = waterColumnEquivalentHeightAbsolute*100))

#Estimate Water Level~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("QB Wetland Well Shallow", df, wells, survey) %>%
  filter(measurement == "Water Depth (m)") %>%
  select(diff)

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+100))

#Insert into the db~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "QB Wetland Well Shallow",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='db_get_ts'])

#5.4 Process upland water level data-----------------------------------------------

#BB Upland Well ------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("BB Upland Well 1", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-df %>% filter(Timestamp > mdy("11/30/2017"))

#Estimate Water Depth~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("BB Upland Well 1", df, wells, survey) %>%
  filter(measurement == "downlaod" & diff < -1) %>%
  group_by(measurement) %>% summarise(diff = mean(diff))

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Calculate water level [Value 0 represents the elevation of the wetland invert]
df$waterLevel<-df$waterDepth+survey$`Upland Well Height (m) - Primary`[survey$Wetland=="BB"]

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        barometricPressure,
                        pressureAbsolute,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+100))

#Insert into the BB~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "BB Upland Well 1",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#DB Upland Well ------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("DB Upland Well 1", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-df %>% filter(Timestamp > mdy("12/1/2017"))

#Estimate Water Depth~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("DB Upland Well 1", df, wells, survey) %>%
  filter(measurement == "downlaod" & diff < -0.9) %>%
  group_by(measurement) %>% summarise(diff = mean(diff))

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Calculate water level [Value 0 represents the elevation of the wetland invert]
df$waterLevel<-df$waterDepth+survey$`Upland Well Height (m) - Primary`[survey$Wetland=="DB"]

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        barometricPressure,
                        pressureAbsolute,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+100))

#Insert into the DB~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "DB Upland Well 1",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#DK Upland Well ------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("DK Upland Well 1", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#remove pre-installation readings. 
df<-df %>% filter(Timestamp > mdy("8/18/2017"))

#Deal with non-overlapping time
df$pressureAbsolute<-if_else(df$Timestamp>ymd("2017-10-20") & df$Timestamp<ymd("2017-12-20"),
                             lead(df$pressureAbsolute, n=77), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#Estimate Water Depth~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("DK Upland Well 1", df, wells, survey) %>%
  filter(measurement == "downlaod" & diff < -2.5) %>%
  group_by(measurement) %>% summarise(diff = mean(diff))

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Calculate water level [Value 0 represents the elevation of the wetland invert]
df$waterLevel<-df$waterDepth+survey$`Upland Well Height (m) - Primary`[survey$Wetland=="DK"]

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        barometricPressure,
                        pressureAbsolute,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+200))

#Insert into the DB~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "DK Upland Well 1",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#ND Upland Well 1------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("ND Upland Well 1", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Deal with non-overlapping time
well_index<-wells %>% dplyr::filter(Site_Name=="ND Upland Well 1") %>% na.omit()
df %>%
  filter(Timestamp>ymd("2017-10-28") & Timestamp<ymd("2017-10-31")) %>%
  select(Timestamp, pressureAbsolute, barometricPressure) %>%
  gather("var","val",-Timestamp) %>%
  group_by(var) %>%
  summarise(min_val = min(val, na.rm=T), 
            date = mean(Timestamp[min_val==val])) %>%
  select(var, date) %>%
  spread(var, date) %>%
  mutate(diff =  barometricPressure - pressureAbsolute) %>% select(diff) %>%
  mutate(lag=diff*12)
df$pressureAbsolute<-if_else(df$Timestamp>ymd("2017-10-20") & df$Timestamp<ymd("2017-12-20"),
                             lead(df$pressureAbsolute, n=59), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        barometricPressure,
                        pressureAbsolute,
                        waterColumnEquivalentHeightAbsolute) %>%
                 mutate(waterColumnEquivalentHeightAbsolute = waterColumnEquivalentHeightAbsolute*100+100))

#Estimate Water Depth~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("ND Upland Well 1", df, wells, survey) %>%
  filter(measurement == "downlaod" & diff < -1.8) %>%
  group_by(measurement) %>% summarise(diff = mean(diff))

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Calculate water level [Value 0 represents the elevation of the wetland invert]
df$waterLevel<-df$waterDepth+survey$`Upland Well Height (m) - Primary`[survey$Wetland=="DK"]

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        barometricPressure,
                        pressureAbsolute,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+200))

#Insert into the DB~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "ND Upland Well 1",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#TB Upland Well 2------------------------------------------------------------
#Download data from files~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-waterHeight_fun("TB Upland Well 2", wells, db, working_dir)

#Conduct any manual edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Outliers
df<-df %>%filter(pressureAbsolute<140)

#Deal with non-overlapping time
#October 10 Download
df$pressureAbsolute<-if_else(df$Timestamp<ymd("2017-10-20"),
                             lead(df$pressureAbsolute, n=60), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972

#December 20 Download
df$pressureAbsolute<-if_else(df$Timestamp>ymd("2017-10-20") & df$Timestamp<ymd("2017-12-20"),
                             lead(df$pressureAbsolute, n=59), #Substract 5 hours
                             df$pressureAbsolute)
df$waterColumnEquivalentHeightAbsolute<-(df$pressureAbsolute-df$barometricPressure)*0.101972




well_index<-wells %>% dplyr::filter(Site_Name=="TB Upland Well 2") %>% na.omit()
df %>%
  filter(Timestamp>ymd("2017-10-27") & Timestamp<ymd("2017-10-31")) %>%
  select(Timestamp, pressureAbsolute, barometricPressure) %>%
  gather("var","val",-Timestamp) %>%
  group_by(var) %>%
  summarise(min_val = min(val, na.rm=T), 
            date = mean(Timestamp[min_val==val])) %>%
  select(var, date) %>%
  spread(var, date) %>%
  mutate(diff =  barometricPressure - pressureAbsolute) %>% select(diff) %>%
  mutate(lag=diff*12)
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        barometricPressure,
                        pressureAbsolute,
                        waterColumnEquivalentHeightAbsolute) %>%
                 mutate(waterColumnEquivalentHeightAbsolute = waterColumnEquivalentHeightAbsolute*100+100))

#Estimate Water Depth~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Estimate offset
offset<-waterDepth_fun("TB UplaTB Well 2", df, wells, survey) %>%
  filter(measurement == "downlaod" & diff < -1.8) %>%
  group_by(measurement) %>% summarise(diff = mean(diff))

#Caclulate water depth
df$waterDepth<-df$waterColumnEquivalentHeightAbsolute+offset$diff

#Calculate water level [Value 0 represents the elevation of the wetlaTB invert]
df$waterLevel<-df$waterDepth+survey$`UplaTB Well Height (m) - Primary`[survey$WetlaTB=="DK"]

#Plot the data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
dygraph_ts_fun(df %>%
                 select(Timestamp,
                        barometricPressure,
                        pressureAbsolute,
                        waterDepth) %>%
                 mutate(waterDepth = waterDepth*100+200))

#Insert into the DB~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Database insert function
t0<-Sys.time()
rodm2::db_insert_results_ts(db = db,
                            datavalues = df,
                            method = "waterdepth",
                            site_code = "TB UplaTB Well 1",
                            processinglevel = "Raw data",
                            sampledmedium = "Liquid aqueous", # from controlled vocab
                            #actionby = "Nate",
                            #equipment_name = "10808360",
                            variables = list( # variable name CV term = list("colname", units = "CV units")
                              "waterLevel" = list(column = "waterLevel", units = "Meter"),
                              "waterDepth" = list(column = "waterDepth", units = "Meter"),
                              "Temperature" = list(column = "temp", units = "Degree Celsius")))
tf<-Sys.time()
tf-t0

#Clean up workspace
remove(list=ls()[ls()!='working_dir' &
                   ls()!='db' &
                   ls()!='files' &
                   ls()!='wells' &
                   ls()!='survey' &
                   ls()!='db_get_ts' &
                   ls()!='waterDepth_fun' &
                   ls()!='waterHeight_fun' &
                   ls()!='dygraph_ts_fun'])

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 6: Pring data----------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Pull data from DB
BB<-db_get_ts(db, "BB Wetland Well Shallow", variable_code_CV = "waterDepth", mdy("10/1/2017"), mdy("9/30/2018")) %>%
  mutate(site="BB Wetland Well Shallow")
DB<-db_get_ts(db, "DB Wetland Well Shallow", variable_code_CV = "waterDepth", mdy("10/1/2017"), mdy("9/30/2018")) %>%
  mutate(site="DB Wetland Well Shallow")
DK<-db_get_ts(db, "DK Wetland Well Shallow", variable_code_CV = "waterDepth", mdy("10/1/2017"), mdy("9/30/2018")) %>%
  mutate(site="DK Wetland Well Shallow")
GN<-db_get_ts(db, "GN Wetland Well Shallow", variable_code_CV = "waterDepth", mdy("10/1/2017"), mdy("9/30/2018")) %>%
  mutate(site="GN Wetland Well Shallow")
ND<-db_get_ts(db, "ND Wetland Well Shallow", variable_code_CV = "waterDepth", mdy("10/1/2017"), mdy("9/30/2018")) %>%
  mutate(site="ND Wetland Well Shallow")
TB<-db_get_ts(db, "TB Wetland Well Shallow", variable_code_CV = "waterDepth", mdy("10/1/2017"), mdy("9/30/2018")) %>%
  mutate(site="TB Wetland Well Shallow")
QB<-db_get_ts(db, "QB Wetland Well Shallow", variable_code_CV = "waterDepth", mdy("10/1/2017"), mdy("9/30/2018")) %>%
  mutate(site="QB Wetland Well Shallow")

#Create one large dataframe
df<-bind_rows(BB, DB, DK, GN, ND, TB, QB)

#tidy
df<-df %>%
  mutate(day = floor_date(Timestamp,"day")) %>%
  select(day, site, waterDepth) %>%
  group_by(day, site) %>%
  summarise(waterDepth=mean(waterDepth, na.rm=T)) %>%
  spread(site, waterDepth)

#export
write_csv(df, paste0(working_dir,"initial_output.csv"))

#Disconnect from db
dbDisconnect(db)




