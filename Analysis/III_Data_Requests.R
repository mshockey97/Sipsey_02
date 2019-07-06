#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name: PT Data Requests
# Coder: C. Nathan Jones
# Date: 14 May 2019
# Purpose: Now that the data is ready, some initial requests
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
source("functions/db_get_ts.R")

#Define working dir
working_dir<-"//nfs/palmer-group-data/Choptank/Nate/PT_Data/"

#Set system time zone 
Sys.setenv(TZ="America/New_York")

#Define database connection
db<-dbConnect(RSQLite::SQLite(),"//nfs/palmer-group-data/Choptank/Nate/PT_Data/choptank.sqlite")

#Define sites
sites<-read_csv(paste0(working_dir,"Database Information/sites.csv")) %>% 
  filter(site_code!="TC Wetland Well Shallow")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#2.0 Download Function--------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
fun<-function(site, 
              start_date = mdy("10-1-2017"), 
              end_date = mdy("9-30-2018")){
  #download data
  temp<-db_get_ts(db, paste(site), "waterLevel", start_date, end_date) %>% as_tibble(.)
  
  #add site collumn
  colnames(temp)<-c("Timestamp", "waterLevel")
  temp$site = paste(site)
  
  #Export to .GlovalEnv
  temp 
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#3.0 Data requests------------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Kelly (All surface water weltand wells)--------------------------------------------
kelly<-sites %>% filter(str_detect(site_code,"Wetland Well Shallow")) %>% as.matrix(.)

#Download Data
kelly<-lapply(kelly, fun) %>% bind_rows(.)

#Sort Data
kelly<-kelly %>% 
  mutate(Timestamp = floor_date(Timestamp, "day")) %>% 
  group_by(site, Timestamp) %>% distinct(.) %>%
  summarise(waterLevel=mean(waterLevel)) %>% 
  spread(site,waterLevel)

write.csv(kelly,paste0(working_dir,"4Kelly.csv"))

#Michael (DK Wetland Wells)--------------------------------------------
MW<-sites %>% filter(str_detect(site_code,"DK")) %>% as.matrix(.)

#Download Data
MW<-lapply(MW, fun) %>% bind_rows(.)

#Sort Data
MW<-MW %>% 
  mutate(Timestamp = floor_date(Timestamp, "day")) %>% 
  group_by(site, Timestamp) %>% distinct(.) %>%
  summarise(waterLevel=mean(waterLevel)) %>% 
  spread(site,waterLevel)

write.csv(MW,paste0(working_dir,"4Michael.csv"))

#Sangchul-----------------------------------------------------------------------
#Obtain data from all surface water weltand wells~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ars<-sites %>% filter(str_detect(site_code,"Wetland Well Shallow")) %>% as.matrix(.)

#Download Data
ars<-lapply(ars, fun) %>% bind_rows(.)

#Sort Data
ars<-ars %>% 
  mutate(Timestamp = floor_date(Timestamp, "15 min")) %>% 
  group_by(site, Timestamp) %>% distinct(.) %>%
  summarise(waterLevel=mean(waterLevel)) %>% 
  spread(site,waterLevel)

write.csv(ars,paste0(working_dir,"4Sangchul.csv"))

#Obtain data from all upland water weltand wells~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ars<-sites %>% filter(str_detect(site_code, "Upland")) %>% as.matrix(.)

fun<-function(site, 
              start_date = mdy("10-1-2017"), 
              end_date = mdy("9-30-2018")){
  #download data
  temp<-db_get_ts(db, paste(site), "waterDepth", start_date, end_date) %>% as_tibble(.)
  
  #add site collumn
  colnames(temp)<-c("Timestamp", "waterDepth")
  temp$site = paste(site)
  
  #Export to .GlovalEnv
  temp 
}

#Download Data
ars<-lapply(ars, fun) %>% bind_rows(.)

#Sort Data
ars<-ars %>% 
  mutate(Timestamp = floor_date(Timestamp, "15 min")) %>% 
  group_by(site, Timestamp) %>% distinct(.) %>%
  summarise(waterDepth=mean(waterDepth)) %>% 
  spread(site,waterDepth)

#export
write.csv(ars,paste0(working_dir,"4Sangchul.csv"))

