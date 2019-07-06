#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Name: Offset Fun
# Coder: C. Nathan Jones
# Date: May 1, 2019
# Purpose: Change timestamp of water level data
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
offset_fun<-function(df, download, path, offset){
  
  #Estimate Offset
  df<-df %>% filter(download_date!= download)
  
  #Redownload data
  ts<-download_fun(path)
  
  #Remove offset
  ts$Timestamp<-ts$Timestamp+offset
  ts$download_date = download
  
  #Bind rows and reorder
  df<-bind_rows(df,ts) %>%
    arrange(Timestamp)
  
  #Export df
  df
}


#1 Day offest in January 2018
# df<-offset_fun(df = df,
#                download = "2018-03-04",
#                path = paste0(working_dir,well_log$path[3]),
#                offset = hours(24))