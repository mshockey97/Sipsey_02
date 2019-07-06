#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name: Pressure Gage Function   
# Coder: C. Nathan Jones
# Date: 29 April 2019
# Purpose: Combine PT and Barometric data to estimate Gage Pressure 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
waterHeight_fun<-function(Timestamp, 
                         pressureAbsolute, 
                         barometricPressure, 
                         temp,
                         download_date_ts,
                         download_date_log,
                         start_date, 
                         end_date, 
                         download_datetime, 
                         force_diff = vector()){
  
  #Organize workspace~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #Create tibble with ts
  df<-tibble(Timestamp, pressureAbsolute, barometricPressure, temp, download_date = download_date_ts)
  
  #If offset dosn't exist
  if(length(force_diff)==0){
    force_diff<-rep(NA, length(download_date_log))
  }
  
  #Create tibble with periods
  well_log<-tibble(download_date=download_date_log, start_date, end_date, download_datetime, force_diff) %>%
    mutate(log_diff = difftime(download_datetime, end_date, units="hours"), 
           log_diff = round(log_diff, 0), 
           est_diff = 0)
  
  #Clip ts to time period
  df<- df %>% 
    filter(Timestamp>min(well_log$start_date, na.rm=T), 
           Timestamp<max(well_log$end_date  , na.rm=T)) 
  
  #Filter potential outliers~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  df<-df %>% 
    #Estimate water height equivolent
    mutate(waterHeight = pressureAbsolute*0.101972) %>%
    #Estimate difference
    mutate(diff = abs(lead(waterHeight) - waterHeight)) %>%
    #Identify failure points where signal "drops
    mutate(diff = if_else(diff< 0.05, 0, 1)) %>%
    mutate(diff = rollapply(diff, 5, sum, align='right', fill=NA)) %>%
    filter(diff == 0)
  
  #Zipper function [minimize variability]~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  zipper_fun<-function(n){
    
    #truncate well_log
    well_log<-well_log[n,]
    
    #Select time period of interest
    ts<-df %>%
      #filter to download date
      filter(download_date == well_log$download_date) %>%
      #Select relevant collumns
      select(Timestamp, barometricPressure, pressureAbsolute) 
    
    #Estiamte dP
    ts<-ts %>%
      mutate(dB = barometricPressure - lag(barometricPressure),
             dA = pressureAbsolute   - lag(pressureAbsolute))
  
    #Develop function to miinimize variability over ts
    inside_fun<-function(window){
      #Etimate water height
      if(window>=0){
        x<-window
        waterHeight<-(lag(ts$pressureAbsolute, x) - ts$barometricPressure)*0.101972
        diff<- abs(lag(ts$dA, x) - ts$dB)
      }else{
        x<-window*-1
        waterHeight<-(lead(ts$pressureAbsolute, x) - ts$barometricPressure)*0.101972
        diff<- abs(lead(ts$dA, x) - ts$dB)
      }
      
      #form daily tibble
      temp<-tibble(Timestamp = ts$Timestamp, waterHeight, diff) %>%
        mutate(Timestamp = floor_date(Timestamp, "day")) %>%
        group_by(Timestamp) %>%
        summarise(var1 = var(diff,na.rm=T),
                  var2 = var(waterHeight, na.rm=T))
      
      #estimate standard deviation
      var<-median(temp$var1,na.rm=T)*median(temp$var2, na.rm=T)
      
      #Export 
      tibble(window, var)
    }
    
    #apply inside fun [on a one hour timestep]
    diff<-lapply(seq(-100,100), inside_fun) %>% 
      bind_rows() %>% 
      filter(var==min(var, na.rm=T)) %>%
      select(window)
    
    #estimate time step
    dt<-median(ts$Timestamp-lag(ts$Timestamp),na.rm=T)
    
    #add step to well log
    well_log$est_diff<-diff$window*dt
    
    #Export well log
    well_log
  }
  
  #Execute zipper_fun  
  well_log<-mclapply(X = seq(1,nrow(well_log)), 
                     FUN = zipper_fun) %>% 
    bind_rows() %>% 
    arrange(start_date)
  
  #Button function [apply offset from zipper fun]~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  button_fun<-function(n){
    
    #truncate well_log
    well_log<-well_log[n,]
    
    #Select time period of interest
    ts<-df %>%
      #filter to download date
      filter(download_date == well_log$download_date) %>%
      #Select relevant collumns
      select(Timestamp, barometricPressure, pressureAbsolute, temp) 
    
    #Define offseet
    if(!is.na(well_log$force_diff)){
      x<-make_difftime(hours = well_log$force_diff)
    }else{
      x<-well_log$est_diff
    }
    
    #Estimate lag
    dt<-median(ts$Timestamp-lag(ts$Timestamp),na.rm=T)
    lags<-abs(as.double(x, units="secs")/as.double(dt, units="secs"))
    
    #Adjust timing on pressure absolute
    if(x>=0){
      ts$pressureAbsolute<-lag(ts$pressureAbsolute, lags) 
    }else{
      ts$pressureAbsolute<-lead(ts$pressureAbsolute, lags) 
    }
    
    #Calculate water depth
    ts$pressureGauge <- ts$pressureAbsolute - ts$barometricPressure
    ts$waterHeight<-ts$pressureGauge*0.101972
    
    #Export ts
    ts
  }
  
  #Apply zipper fun
  df<-mclapply(seq(1,nrow(well_log)), button_fun) %>% 
    bind_rows() %>% 
    arrange(Timestamp)
  
  #Assign well log to global env
  assign('h_report',well_log, .GlobalEnv)
  
  #Export df
  df
}