#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name: Database Setup 
# Coder: C. Nathan Jones
# Date: 29 April 2019
# Purpose: Setup SQLite Database to Organize Water Level Data
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

#Disconnect from DB
dbDisconnect(db)
