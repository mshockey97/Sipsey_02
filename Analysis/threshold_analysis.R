#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name: Threshold Analysis
# Coder: C. Nathan Jones
# Date: 6 July 2019
# Purpose: Estimate periods when water levels are above a give threshold
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
source("R/db_get_ts.R")

#Define working dir
working_dir<-"//nfs/njones-data/Research Projects/SWI/PT_Data/"

#Define output directory
output_dir<-"//nfs/njones-data/Research Projects/SWI/Threshold_Analysis/"

#Set system time zone 
Sys.setenv(TZ="America/New_York")

#Define database connection
db<-dbConnect(RSQLite::SQLite(),paste0(working_dir, "SWI.sqlite"))

#Define GW sites
sites<-tibble(site_code = db_get_sites(db)) %>% 
  filter(str_detect(site_code,"-A") |
         str_detect(site_code,"-B") |
         str_detect(site_code,"-C")) %>%
  filter(!str_detect(site_code,"EL-")) %>%
  as.matrix(.)
  
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#2.0 Download data--------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Develop download function
fun<-function(site, 
              start_date = mdy("8-24-2018"), 
              end_date   = mdy("3-7-2019")){
  #download data
  temp<-db_get_ts(db, paste(site), "waterDepth", start_date, end_date) %>% as_tibble(.)
  
  #add site collumn
  colnames(temp)<-c("Timestamp", "waterLevel")
  temp$site = paste(site)
  
  #Export to .GlovalEnv
  temp 
}

#Apply function
df<-lapply(sites, fun) %>% bind_rows(.)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#3.0 Analysis-------------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#3.1 Aggregate by day and site--------------------------------------------------
#Tidy data
df<-df %>%
  mutate(Timestamp = floor_date(Timestamp,'day'),
         site = substr(site, 1,2)) %>%
  group_by(site, Timestamp) %>%
  summarise(waterLevel=mean(waterLevel, na.rm=T))

#plot
df %>% 
  #Start Plotting Device
  ggplot(aes(x=Timestamp, y=waterLevel)) +
  #Start facet   
  facet_grid(site~.) +
  #Add line
  geom_line() +
  #Add Theme
  theme_bw() +
    labs(x="Timestamp", y= "Water Depth [m]")
ggsave(paste0(output_dir, "hydrograph.png"), width=5, height=7, units="in")


#3.2 Estimate Time avoe threshold-----------------------------------------------
#Define Threshold
threshold<- -0.15 #05 cm

#Estimate duration above threshold
df<-df %>%
  filter(Timestamp>mdy("11/1/2018")) %>%
  filter(waterLevel > threshold) %>%
  group_by(site) %>%
  tally() %>% 
  arrange(n)

#Plot
df$site<-factor(df$site, levels=c("EP", "AL", "GR", "BN"))
df %>% 
  ggplot(aes(x=site, y=n))+
    geom_bar(stat = "identity") +
    theme_bw() +
      labs(x="Site", y="Days above threshold")
ggsave(paste0(output_dir, "threshold.png"), width=3.5, height=3, units="in")



