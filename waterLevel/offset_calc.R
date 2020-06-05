#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name: Offset 
# Coder: C. Nathan Jones (cnjones7@ua.edu)
# Date: 6/4/2020
# Purpose: Determine vetical offset for each gage
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 1: Setup workspace-------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Clear workspace 
remove(list=ls())

#Gather libraries of interest
library(lubridate)
library(tidyverse)

#Read custom R functions
source("R/download_fun.R")

#define master dir
master_dir<-"C:\\Users\\cnjones7\\Box Sync\\My Folders\\Research Projects\\SWI\\PT_Data\\"

#List files 
files<-list.files(master_dir, full.names =  TRUE, recursive = T)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 2: Download and organize data--------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#2.1 Read all pt data-----------------------------------------------------------
pt_files<-files[str_detect(files,'xport')]
pt_data<-lapply(pt_files, download_fun) %>% bind_rows()

#2.2 Log Files------------------------------------------------------------------
#Download log files
log_files<-files[str_detect(files, "log")]
log_data<-lapply(log_files, read_csv) %>% bind_rows()
log_data<-log_data %>% 
  mutate(Timestamp = mdy_hms(paste(Date,Time))) %>% 
  mutate(Timestamp = round_date(Timestamp, 'hours')) %>% 
  select(Site_Name, Sonde_ID, Timestamp, Relative_Water_Level_m)

#2.3 Baro data------------------------------------------------------------------
#First period use only BN data [GR data logging at 6 hr intervals]
baro_1<-download_fun("C:\\Users\\cnjones7\\Box Sync\\My Folders\\Research Projects\\SWI\\PT_Data\\20190315_Downloads\\export\\SWI_BN_Baro.csv")

#Define baro loggers during 3rd download
baro_files<-files[str_detect(files, "aro") & str_detect(files, ".csv")]
baro_files<-baro_files[str_detect(baro_files, "20190709")]

#Download/organize baro from two sites
gr_baro<-download_fun(baro_files[str_detect(baro_files,"GR")])
gr_baro<-bind_rows(gr_baro, baro_1) %>% arrange(Timestamp)
bn_baro<-download_fun(baro_files[str_detect(baro_files,"BN")])
bn_baro<-bind_rows(bn_baro, baro_1) %>% arrange(Timestamp)

#Gap fill gr baro
gr_baro<-bind_rows(
  gr_baro, bn_baro %>% filter(Timestamp>mdy("3/14/2019"),
                              Timestamp<mdy("5/17/2019")))

#Create list of sites and associated barometric logger
baro_key<-tibble(
  Site_Name = log_data$Site_Name,
  baro = NA) %>% 
  #Greenhead Sites
  mutate(baro = ifelse(str_detect(Site_Name,"GR"), "GR-Baro", baro)) %>% 
  #Elmwood Sites
  mutate(baro = ifelse(str_detect(Site_Name,"EP"), "GR-Baro", baro)) %>% 
  #Almodington
  mutate(baro = ifelse(str_detect(Site_Name,"AL"), "GR-Baro", baro)) %>% 
  #Barneck
  mutate(baro = ifelse(str_detect(Site_Name,"BN"), "BN-Baro", baro)) %>% 
  #Reomve duplicates
  distinct()

#Create interpolation function 
gr_baro_fun<-approxfun(gr_baro$Timestamp, gr_baro$pressureAbsolute)
bn_baro_fun<-approxfun(bn_baro$Timestamp, bn_baro$pressureAbsolute)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 3: Estimate waterDepth --------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Joint to df
pt_data<-pt_data %>% left_join(., log_data) 

#Assign baro pressure
pt_data<-pt_data %>% 
  #Add baro key data
  left_join(., baro_key) %>% 
  #Interpolate barometric pressure from loggers
  mutate(
    gr_baro = gr_baro_fun(Timestamp),
    bn_baro = bn_baro_fun(Timestamp)) %>% 
  #Based on baro_key, assign baro pressure
  mutate(pressureBaro = if_else(baro == 'GR-Baro',gr_baro,bn_baro)) %>% 
  #Clean up
  select(Site_Name, Timestamp, pressureAbsolute, pressureBaro)

#Estimate waterHeight
pt_data<-pt_data %>% 
  mutate(pressureGauge = pressureAbsolute-pressureBaro, 
         waterHeight   = pressureGauge/9.81, 
         Timestamp = round_date(Timestamp, 'day')) %>% 
  drop_na(Site_Name) %>% 
  group_by(Site_Name, Timestamp) %>% 
  summarise(waterHeight = mean(waterHeight, na.rm=T))
  
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Step 4: Estimate offset --------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Join with pt data
offset<-log_data %>% 
  mutate(Timestamp = round_date(Timestamp, 'day')) %>% 
  left_join(., pt_data, by=c('Site_Name', "Timestamp")) %>% 
  select(Site_Name, Timestamp, Relative_Water_Level_m, waterHeight)

#Calculate observed offset
offset<-offset %>% 
  mutate(offset = Relative_Water_Level_m - waterHeight) %>% 
  arrange(Site_Name) %>% 
  drop_na()

#Look at offset
offset %>% View()

#Summarize by mean
offset<-offset %>% 
  group_by(Site_Name) %>% 
  summarise(offset=mean(offset, na.rm=T)) 

#Export offset information
write_csv(offset, paste0(master_dir,"offset.csv"))
