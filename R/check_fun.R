#Grab info from files
file_fun<-function(n){
  
  #Download data
  temp<-read_csv(pt_files[n], skip=1)
  
  #Determine serial number
  serial_number<-colnames(temp)[grep("LGR",colnames(temp))][1]  #Find collumn name with serial number
  serial_number<-substr(serial_number,   #isolate serial number
                        gregexpr("SEN.S.N",serial_number)[[1]][1]+9, #Start
                        nchar(serial_number)-1) #stop
  serial_number<-as.numeric(serial_number) 
  
  #Determine TZ
  time_zone<-colnames(temp)[grep("GMT",colnames(temp))]  #Grab collumn name w/ time offset
  time_zone<-substr(time_zone,
                    regexpr('GMT', time_zone)[1],
                    nchar(time_zone))
  time_zone<-if_else(time_zone=="GMT-04:00",
                     "EST",
                     if_else(time_zone=="GMT-05:00",
                             "EDT",
                             "-9999"))
  #Determin units
  units<-colnames(temp)[grep("Abs Pres,",colnames(temp))]
  units<-substr(units,
                regexpr("Abs Pres,", units)+10,
                regexpr("Abs Pres,", units)+12)
  
  #Organize
  colnames(temp)<-c("ID","Timestamp","pressureAbsolute", "temp")
  temp<-temp[,c("Timestamp","pressureAbsolute", "temp")]
  temp<-temp %>%
    #Select collumns of interest
    dplyr::select(Timestamp, pressureAbsolute, temp) %>%
    #Convert to POSIX
    dplyr::mutate(Timestamp = as.POSIXct(strptime(Timestamp, "%m/%d/%y %I:%M:%S %p"), tz = time_zone))  %>%
    #Convert to GMT
    dplyr::mutate(Timestamp = with_tz(Timestamp, "GMT")) %>%
    #Order the intput
    dplyr::arrange(Timestamp)
  
  #create output
  tibble(path       = pt_files[n], 
         Sonde_ID   = serial_number,
         units      = units, 
         start_date = min(temp$Timestamp), 
         end_date   = max(temp$Timestamp))
}

#check fun
check_fun<-function(pt_files,field_log){
  #Collect sonde serial number from sonde download files
  sonde_files<-lapply(X = seq(1,length(pt_files)), FUN = file_fun)
  sonde_files<-bind_rows(sonde_files) %>% select(Sonde_ID)
  
  #Collect sonde serial number from field sheet
  field_sheet<-field_log %>% select(Sonde_ID)
  
  #Join
  sonde_files <- sonde_files %>% mutate(x = 'sonde_files')
  field_sheet <- field_sheet %>% mutate(y = 'field_sheet')
  output<-full_join(sonde_files,field_sheet)
  
  #Find missing data
  output<-output %>% filter(is.na(x) | is.na(y))
  
  #Export Output
  if(nrow(output)==0){
    print("#--------------------------------------------------")
    print("#--------------------------------------------------")
    print("#--------------------------------------------------")
    print("Looks like your field sheets match the downloads")
  }else{
    print("#--------------------------------------------------")
    print("#--------------------------------------------------")
    print("#--------------------------------------------------")
    print("Ahh shit...you messed something up you goof! ")
    output
  }
}
  