#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Title: waterLevel 
#Coder: Nate Jones (cnjones7@ua.edu)
#Date: 5/28/2020
#Purpose: Re-analysis of water level data downloaded Sept 2018
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#Table of Contents~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1: Organize workspace
# Step 2: Field Worksheet
# Step 3: Baro Data
# Step 4: Water Depth
# Step 5: QAQC
# Step 6: Print

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1: Setup workspace-------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#Clear workspace 
remove(list=ls())

#Gather libraries of interest
library(dygraphs)
library(lubridate)
library(zoo)
library(tidyverse)

#Read custom R functions
source("R/download_fun.R")
source("R/check_fun.R")
source("R/dygraph_ts_fun.R")
source("R/dygraph_QAQC_fun.R")

#Define data directory
data_dir<-"C:\\Users\\cnjones7\\Box Sync\\My Folders\\Research Projects\\SWI\\PT_Data\\20180919_Downloads\\"

#list pt and baro file locations
pt_files<-list.files(paste0(data_dir, "export"), full.names =  TRUE)
baro_files<-"C:\\Users\\cnjones7\\Box Sync\\My Folders\\Research Projects\\SWI\\PT_Data\\20190315_Downloads\\export\\SWI_BN_Baro.csv"
field_files<-paste0(data_dir, 'well_log.csv')
offset<-read_csv("C:\\Users\\cnjones7\\Box Sync\\My Folders\\Research Projects\\SWI\\PT_Data\\offset.csv")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 2: Field Worksheet--------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Download Field Worksheet
field_log<-read_csv(field_files)

#Check to make sure pt files match field logs
check_fun(pt_files,field_log)
    #If final 

#create df of site name, sonde_id, and measured offset
field_log<-field_log %>% 
  select(Site_Name, Sonde_ID, Relative_Water_Level_m)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3: Barometric Pressure Data----------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Gather baro data
baro<-download_fun(baro_files)

#Plot to check for any issues
baro %>% select(Timestamp, pressureAbsolute) %>%  dygraph_ts_fun()
  #There are some weird spikes on near Sept 30?

#Create interpolation function 
baro_fun<-approxfun(baro$Timestamp, baro$pressureAbsolute)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4: WaterDepth Data-------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Gather PT data
df<-lapply(pt_files, download_fun) %>% bind_rows()

#Joint to df
df<-df %>% left_join(., field_log) 

#Estimate waterHeight
df<-df %>% 
  mutate(pressureBaro  = baro_fun(Timestamp), 
         pressureGauge = pressureAbsolute-pressureBaro, 
         waterHeight   = pressureGauge/9.81)

#Joint to df
df<-df %>% left_join(., offset) 

#Estimate waterdepth
df<-df %>% mutate(waterDepth = waterHeight + offset)

#Subset to waterDepth (after inspection!)
df<-df %>% select(Timestamp, Site_Name, waterDepth) 

#Add prcessing level
df$processing_level<-"raw"
  
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 5: QAQC------------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#list site names
field_log %>% select(Site_Name) %>% arrange(Site_Name) %>% pull()

#5.1 BN-A-----------------------------------------------------------------------
#Organize data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
site<-"BN-A"
waterDepth <- df %>% filter(Site_Name==site) %>% select(Timestamp, waterDepth)
dygraph_QAQC_fun(waterDepth)

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add rolling average
updated<-waterDepth %>% 
  mutate(waterDepth = rollmean(waterDepth, k=5, fill=NA))

#remove weird periods
updated<-updated %>% 
  filter(Timestamp < mdy_hms("9/7/2018, 23:00:00") |
         Timestamp > mdy_hms("9/8/2018, 17:45:00")) %>% 
  filter(Timestamp < mdy_hms("9/11/2018, 5:45:00") |
           Timestamp > mdy_hms("9/11/2018, 23:45:00")) %>%
  filter(Timestamp < mdy_hms("9/12/2018, 5:00:00") |
           Timestamp > mdy_hms("9/12/2018, 23:00:00")) 

#interpolate between weird periods
interpfun<-approxfun(updated$Timestamp, updated$waterDepth)  
updated<-tibble(
    Timestamp = waterDepth$Timestamp,
    waterDepth = interpfun(waterDepth$Timestamp))

#Inspect output
dygraph_QAQC_fun(waterDepth, historic=NULL, updated)

#print~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add info to 'updated' dataframe
updated$Site_Name = site
updated$processing_level = 'processed'

#Append master dataframe
df<-bind_rows(df, updated)

#5.2 BN-B-----------------------------------------------------------------------
#Organize data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
site<-"BN-B"
waterDepth <- df %>% filter(Site_Name==site) %>% select(Timestamp, waterDepth)
dygraph_QAQC_fun(waterDepth)

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove bogus point
updated<-waterDepth %>% 
  filter(Timestamp!=mdy_hm("9/6/2018 19:15"))

#Add rolling average
updated<-updated %>% 
  mutate(waterDepth = rollmean(waterDepth, k=5, fill=NA))

#remove weird periods
updated<-updated %>% 
  filter(Timestamp < mdy_hms("9/7/2018, 23:00:00") |
           Timestamp > mdy_hms("9/8/2018, 17:45:00")) %>% 
  filter(Timestamp < mdy_hms("9/11/2018, 5:45:00") |
           Timestamp > mdy_hms("9/11/2018, 23:45:00")) %>%
  filter(Timestamp < mdy_hms("9/12/2018, 5:00:00") |
           Timestamp > mdy_hms("9/12/2018, 23:00:00")) 

#interpolate between weird periods
interpfun<-approxfun(updated$Timestamp, updated$waterDepth)  
updated<-tibble(
  Timestamp = waterDepth$Timestamp,
  waterDepth = interpfun(waterDepth$Timestamp))

#Inspect output
dygraph_QAQC_fun(waterDepth, historic=NULL, updated)

#print~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add info to 'updated' dataframe
updated$Site_Name = site
updated$processing_level = 'processed'

#Append master dataframe
df<-bind_rows(df, updated)

#5.3 BN-C-----------------------------------------------------------------------
#Organize data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
site<-"BN-C"
waterDepth <- df %>% filter(Site_Name==site) %>% select(Timestamp, waterDepth)
dygraph_QAQC_fun(waterDepth)

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add rolling average
updated<-waterDepth %>% 
  mutate(waterDepth = rollmean(waterDepth, k=5, fill=NA))

#remove weird periods
updated<-updated %>% 
  filter(Timestamp < mdy_hms("9/7/2018, 23:00:00") |
           Timestamp > mdy_hms("9/8/2018, 17:45:00")) %>% 
  filter(Timestamp < mdy_hms("9/11/2018, 5:45:00") |
           Timestamp > mdy_hms("9/11/2018, 23:45:00")) %>%
  filter(Timestamp < mdy_hms("9/12/2018, 5:00:00") |
           Timestamp > mdy_hms("9/12/2018, 23:00:00")) 

#interpolate between weird periods
interpfun<-approxfun(updated$Timestamp, updated$waterDepth)  
updated<-tibble(
  Timestamp = waterDepth$Timestamp,
  waterDepth = interpfun(waterDepth$Timestamp))

#Inspect output
dygraph_QAQC_fun(waterDepth, historic=NULL, updated)

#print~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add info to 'updated' dataframe
updated$Site_Name = site
updated$processing_level = 'processed'

#Append master dataframe
df<-bind_rows(df, updated)

#5.4 EP-A-----------------------------------------------------------------------
#Organize data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
site<-"EP-A"
waterDepth <- df %>% filter(Site_Name==site) %>% select(Timestamp, waterDepth)
dygraph_QAQC_fun(waterDepth)

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add rolling average
updated<-waterDepth %>% 
  mutate(waterDepth = rollmean(waterDepth, k=5, fill=NA))

#remove weird periods
updated<-updated %>% 
  filter(Timestamp < mdy_hms("9/8/2018, 2:00:00") |
           Timestamp > mdy_hms("9/8/2018, 17:45:00")) %>% 
  filter(Timestamp < mdy_hms("9/11/2018, 5:45:00") |
           Timestamp > mdy_hms("9/11/2018, 23:45:00")) %>%
  filter(Timestamp < mdy_hms("9/12/2018, 5:00:00") |
           Timestamp > mdy_hms("9/12/2018, 23:00:00")) 

#interpolate between weird periods
interpfun<-approxfun(updated$Timestamp, updated$waterDepth)  
updated<-tibble(
  Timestamp = waterDepth$Timestamp,
  waterDepth = interpfun(waterDepth$Timestamp))

#Inspect output
dygraph_QAQC_fun(waterDepth, historic=NULL, updated)

#print~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add info to 'updated' dataframe
updated$Site_Name = site
updated$processing_level = 'processed'

#Append master dataframe
df<-bind_rows(df, updated)

#5.5 EP-B-----------------------------------------------------------------------
#Organize data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
site<-"EP-B"
waterDepth <- df %>% filter(Site_Name==site) %>% select(Timestamp, waterDepth)
dygraph_QAQC_fun(waterDepth)

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add rolling average
updated<-waterDepth %>% 
  mutate(waterDepth = rollmean(waterDepth, k=5, fill=NA))

#remove weird periods
updated<-updated %>% 
  filter(Timestamp < mdy_hms("9/8/2018, 2:00:00") |
           Timestamp > mdy_hms("9/8/2018, 17:45:00")) %>% 
  filter(Timestamp < mdy_hms("9/11/2018, 5:45:00") |
           Timestamp > mdy_hms("9/11/2018, 23:45:00")) %>%
  filter(Timestamp < mdy_hms("9/12/2018, 5:00:00") |
           Timestamp > mdy_hms("9/12/2018, 23:00:00")) 

#interpolate between weird periods
interpfun<-approxfun(updated$Timestamp, updated$waterDepth)  
updated<-tibble(
  Timestamp = waterDepth$Timestamp,
  waterDepth = interpfun(waterDepth$Timestamp))

#Inspect output
dygraph_QAQC_fun(waterDepth, historic=NULL, updated)

#print~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add info to 'updated' dataframe
updated$Site_Name = site
updated$processing_level = 'processed'

#Append master dataframe
df<-bind_rows(df, updated)

#5.6 EP-C-----------------------------------------------------------------------
#Organize data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
site<-"EP-C"
waterDepth <- df %>% filter(Site_Name==site) %>% select(Timestamp, waterDepth)
dygraph_QAQC_fun(waterDepth)

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove bogus point
updated<-waterDepth %>% 
  filter(Timestamp!=mdy_hm("9/6/2018 14:45"))

#Add rolling average
updated<-updated %>% 
  mutate(waterDepth = rollmean(waterDepth, k=5, fill=NA))

#remove weird periods
updated<-updated %>% 
  filter(Timestamp < mdy_hms("9/8/2018, 2:00:00") |
           Timestamp > mdy_hms("9/8/2018, 17:45:00")) %>% 
  filter(Timestamp < mdy_hms("9/11/2018, 5:45:00") |
           Timestamp > mdy_hms("9/11/2018, 23:45:00")) %>%
  filter(Timestamp < mdy_hms("9/12/2018, 5:00:00") |
           Timestamp > mdy_hms("9/12/2018, 23:00:00")) 

#interpolate between weird periods
interpfun<-approxfun(updated$Timestamp, updated$waterDepth)  
updated<-tibble(
  Timestamp = waterDepth$Timestamp,
  waterDepth = interpfun(waterDepth$Timestamp))

#Inspect output
dygraph_QAQC_fun(waterDepth, historic=NULL, updated)

#print~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add info to 'updated' dataframe
updated$Site_Name = site
updated$processing_level = 'processed'

#Append master dataframe
df<-bind_rows(df, updated)

#6.7 GR-A-----------------------------------------------------------------------
#Organize data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
site<-"GR-A"
waterDepth <- df %>% filter(Site_Name==site) %>% select(Timestamp, waterDepth)
dygraph_QAQC_fun(waterDepth)

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add rolling average
updated<-waterDepth %>% 
  mutate(waterDepth = rollmean(waterDepth, k=5, fill=NA))

#remove weird periods
updated<-updated %>% 
  filter(Timestamp < mdy_hms("9/7/2018, 23:00:00") |
           Timestamp > mdy_hms("9/8/2018, 17:45:00")) %>% 
  filter(Timestamp < mdy_hms("9/11/2018, 5:45:00") |
           Timestamp > mdy_hms("9/11/2018, 23:45:00")) %>%
  filter(Timestamp < mdy_hms("9/12/2018, 5:00:00") |
           Timestamp > mdy_hms("9/12/2018, 23:00:00")) 

#interpolate between weird periods
interpfun<-approxfun(updated$Timestamp, updated$waterDepth)  
updated<-tibble(
  Timestamp = waterDepth$Timestamp,
  waterDepth = interpfun(waterDepth$Timestamp))

#Inspect output
dygraph_QAQC_fun(waterDepth, historic=NULL, updated)

#print~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add info to 'updated' dataframe
updated$Site_Name = site
updated$processing_level = 'processed'

#Append master dataframe
df<-bind_rows(df, updated)

#6.8 GR-B-----------------------------------------------------------------------
#Organize data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
site<-"GR-B"
waterDepth <- df %>% filter(Site_Name==site) %>% select(Timestamp, waterDepth)
dygraph_QAQC_fun(waterDepth)

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add rolling average
updated<-waterDepth %>% 
  mutate(waterDepth = rollmean(waterDepth, k=5, fill=NA))

#remove weird periods
updated<-updated %>% 
  filter(Timestamp < mdy_hms("9/7/2018, 23:00:00") |
           Timestamp > mdy_hms("9/8/2018, 17:45:00")) %>% 
  filter(Timestamp < mdy_hms("9/11/2018, 5:45:00") |
           Timestamp > mdy_hms("9/11/2018, 23:45:00")) %>%
  filter(Timestamp < mdy_hms("9/12/2018, 5:00:00") |
           Timestamp > mdy_hms("9/12/2018, 23:00:00")) 

#interpolate between weird periods
interpfun<-approxfun(updated$Timestamp, updated$waterDepth)  
updated<-tibble(
  Timestamp = waterDepth$Timestamp,
  waterDepth = interpfun(waterDepth$Timestamp))

#Inspect output
dygraph_QAQC_fun(waterDepth, historic=NULL, updated)

#print~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add info to 'updated' dataframe
updated$Site_Name = site
updated$processing_level = 'processed'

#Append master dataframe
df<-bind_rows(df, updated)

#6.9 GR-C-----------------------------------------------------------------------
#Organize data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
site<-"GR-C"
waterDepth <- df %>% filter(Site_Name==site) %>% select(Timestamp, waterDepth)
dygraph_QAQC_fun(waterDepth)

#Manual Edits~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add rolling average
updated<-waterDepth %>% 
  mutate(waterDepth = rollmean(waterDepth, k=5, fill=NA))

#remove weird periods
updated<-updated %>% 
  filter(Timestamp < mdy_hms("9/7/2018, 23:00:00") |
           Timestamp > mdy_hms("9/8/2018, 17:45:00")) %>% 
  filter(Timestamp < mdy_hms("9/11/2018, 5:45:00") |
           Timestamp > mdy_hms("9/11/2018, 23:45:00")) %>%
  filter(Timestamp < mdy_hms("9/12/2018, 5:00:00") |
           Timestamp > mdy_hms("9/12/2018, 23:00:00")) 

#interpolate between weird periods
interpfun<-approxfun(updated$Timestamp, updated$waterDepth)  
updated<-tibble(
  Timestamp = waterDepth$Timestamp,
  waterDepth = interpfun(waterDepth$Timestamp))

#Inspect output
dygraph_QAQC_fun(waterDepth, historic=NULL, updated)

#print~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Add info to 'updated' dataframe
updated$Site_Name = site
updated$processing_level = 'processed'

#Append master dataframe
df<-bind_rows(df, updated)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 6: Print-----------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
df<-df %>% select(Timestamp, Site_Name, waterDepth, processing_level) %>% filter(processing_level == 'processed') %>% select(-processing_level)
write_csv(df, paste0(data_dir,"20180919_waterLevel.csv"))
