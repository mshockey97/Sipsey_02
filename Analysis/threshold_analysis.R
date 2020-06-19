#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name: Threshold Analysis
# Coder: C. Nathan Jones
# Date: 6/5/2020
# Purpose: Estimate periods when water levels are above a give threshold
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
                        
#Define output directory
output_dir<-"C:/Users/cnjones7/Box Sync/My Folders/Research Projects/SWI/Threshold_Analysis/"

#Set system time zone 
Sys.setenv(TZ="America/New_York")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#2.0 Download data--------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#DEfine waterLevel files
files<-list.files(data_dir, full.names = TRUE, recursive = TRUE)
files<-files[str_detect(files, "waterLevel")]

#Download data
df<-lapply(files,read_csv) %>% bind_rows()

#Subset to upland well info
upland<-df %>% 
  #Filter by site
  filter(str_detect(Site_Name, "BN") |
         str_detect(Site_Name, "GR")) %>% 
  #Filter by well
  filter(str_detect(Site_Name,"-A") |
         str_detect(Site_Name,"-B") |
         str_detect(Site_Name,"-C")) %>%
  #Filter by date
  filter(Timestamp>mdy_hms("6-5-2019  00:00:01") &
         Timestamp<mdy_hms("7-15-2019 00:00:01"))

#Subset ditch well info 
ditch<-df %>% 
  #Filter by site
  filter(str_detect(Site_Name, "BN") |
           str_detect(Site_Name, "GR")) %>% 
  #Filter by well
  filter(str_detect(Site_Name,"-D")) %>%
  #Filter by date
  filter(Timestamp>mdy_hms("6-5-2019  00:00:01") &
           Timestamp<mdy_hms("7-15-2019 00:00:01"))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#3.0 Analysis-------------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#3.1 Aggregate by day and site--------------------------------------------------
#Tidy data
upland<-upland %>%
  mutate(Timestamp = floor_date(Timestamp,'day'),
         site = substr(Site_Name, 1,2)) %>%
  group_by(site, Timestamp) %>%
  summarise(waterDepth=mean(waterDepth, na.rm=T))
ditch<-ditch %>% 
  mutate(Timestamp = floor_date(Timestamp,"day"), 
         site = substr(Site_Name, 1,2)) %>% 
  group_by(site, Timestamp) %>% 
  summarise(waterDepth=mean(waterDepth, na.rm = T))

#plot
upland_plot<-upland %>% 
  #Start Plotting Device
  ggplot(aes(x=Timestamp, y=waterDepth)) +
  #Start facet   
  facet_grid(site~.) +
  #Add line
  geom_line() +
  #Add Theme
  theme_bw() +
    labs(x="Timestamp", y= "Mean Upland Water Level [m/day]")

ditch_plot<-ditch %>% 
  #Start Plotting Device
  ggplot(aes(x=Timestamp, y=waterDepth)) +
  #Start facet   
  facet_grid(site~.) +
  #Add line
  geom_line() +
  #Add Theme
  theme_bw() +
  labs(x="Timestamp", y= "Ditch Water Level [m/day]")

upland_plot + ditch_plot

#3.2 Estimate Time above threshold----------------------------------------------
#Upland wells~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Define Threshold
threshold<- -0.15 #15 cm

#Estimate duration above threshold
upland %>%
  filter(waterDepth > threshold) %>%
  group_by(site) %>%
  tally() %>% 
  arrange(n)

# #Plot
# upland$site<-factor(upland$site, levels=c("EP", "AL", "GR", "BN"))
# upland %>% 
#   ggplot(aes(x=site, y=n))+
#     geom_bar(stat = "identity") +
#     theme_bw() +
#       labs(x="Site", y="Days above threshold")
# ggsave(paste0(output_dir, "threshold.png"), width=3.5, height=3, units="in")

#SW Inundation~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Define thresholds
threshold<-tibble(
  site = c("BN","GR"),
  threshold_depth = c(0.164592, 0.128016)
)

#join threshold data to ditch data
ditch<-left_join(ditch, threshold)

#Count when waterDepth greater than threshold
ditch %>% 
  mutate(flood = if_else(waterDepth>=threshold_depth, 1, 0)) %>% 
  group_by(site) %>% 
  summarise(flood = sum(flood, na.rm=T))

#3.3 Estimate Metrics----------------------------------------------------------
upland %>% group_by(site) %>% summarise(waterDepth = mean(waterDepth,na.rm=T))
ditch %>% group_by(site) %>% summarise(waterDepth = mean(waterDepth,na.rm=T))

