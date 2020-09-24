#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name: Daily Water Level Change
# Coder: C. Nathan Jones
# Date: 9/23/2020
# Purpose: Create script to estimate daily waterLevel Change 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#1.0 Setup Worskspace-----------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#clear environment
remove(list=ls())

#load relevant packages
library(patchwork)
library(lubridate)
library(tidyverse)

#Define working dir
data_dir<-"C:/Users/cnjones7/Box Sync/My Folders/Research Projects/SWI/PT_Data/"
output_dir<-"C:/Users/cnjones7/Box Sync/My Folders/Research Projects/SWI/Processed_Data/"

#Read in waterLevel data
df<-read.csv(paste0(output_dir, "waterLevel.csv"))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#2.0 Estimate Daily Change------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Remove Ditch wells
df<-df %>% 
  select(-'BN-D',
         -'AL-D',
         -'GR-D')

#Estiamte mean daily water level
df<-df %>% 
  #Isolate year and day
  mutate(date = date(Timestamp_GMT)) %>% 
  select(-Timestamp_GMT) %>% 
  #pivot longer
  pivot_longer(-c(date)) %>% 
  #Isolate site
  mutate(site = substr(name,1,2)) %>% 
  #average by day and site
  group_by(date, site) %>% 
  summarise(waterLevel=mean(value, na.rm=T)) %>% 
  #Pivot wider
  pivot_wider(names_from = 'site', values_from='waterLevel') %>% 
  #Remove groups
  ungroup()
  
#Estimate daily change
df<-df %>% mutate(
  AL = AL - lag(AL),
  BN = BN - lag(BN),
  EP = EP - lag(EP),
  GR = GR - lag(GR)
)

#Export
write.csv(df, paste0(output_dir,"waterLevel_change_m_per_day.csv"))
