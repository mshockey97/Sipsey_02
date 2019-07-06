waterDepth_fun<-function(#from working df
                         Timestamp, 
                         waterHeight,
                         #from well log
                         download_date, 
                         Relative_Water_Level_m){ 

  #Organize Data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  #Water height time series data
  ts<-tibble(Timestamp, waterHeight) %>% 
    mutate(Timestamp = floor_date(Timestamp, "day")) %>%
    group_by(Timestamp) %>%
    summarise(waterHeight = mean(waterHeight, na.rm = T))
  
  #Water depth at download (well log file)
  depths<-tibble(Timestamp = download_date, waterDepth=Relative_Water_Level_m) %>%
    mutate(Timestamp = floor_date(as.POSIXct(Timestamp, tz = "America/New_York"), "day"))
  
  #Estimate water depth during each download~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  output<-left_join(depths, ts) %>%
    mutate(offset = waterDepth - waterHeight, 
           event = "download") %>%
    select(offset, event) %>% na.omit()
  
  #Export output
  output
}