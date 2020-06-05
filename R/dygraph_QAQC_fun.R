dygraph_QAQC_fun<-function(waterDepth, historic=NULL, updated=NULL){

  #Convert to cm and add 2 meters (we want to avoide negative numbers for dygraphs)
  waterDepth$waterDepth<-waterDepth$waterDepth*100 + 2000
  
  #Combine historic and/or updated data if available
  if(!is.null(historic)){
    historic<-historic %>% 
      rename(historic=waterDepth) %>% 
      mutate(historic = historic*100 + 2000)
    waterDepth<-full_join(waterDepth, historic)}
  if(!is.null(updated)){
    updated<-updated %>% 
      rename(updated=waterDepth) %>% 
      mutate(updated = updated*100 + 2000)
    waterDepth<-full_join(waterDepth, updated)}
  
  #Convert to xts format
  waterDepth_xts<-waterDepth %>% 
    arrange(Timestamp) %>% 
    replace_na(list(
      waterDepth = 2000, 
      historic = 2000,
      updated = 2000))
  waterDepth_xts<-xts(waterDepth_xts, order.by=waterDepth_xts$Timestamp)
  waterDepth_xts<-waterDepth_xts[,-1]
  
  #Plot dygraph
  dygraph(waterDepth_xts) %>%
    dyLimit(2000, "Ground Surface") %>% 
    dyRangeSelector() %>%
    dyLegend() %>%
    dyOptions(strokeWidth = 1.5) %>%
    dyOptions(labelsUTC = TRUE) %>%
    dyHighlight(highlightCircleSize = 5,
                highlightSeriesBackgroundAlpha = 0.2,
                hideOnMouseOut = FALSE) %>%
    dyAxis("y", label = "waterDepth [cm]")

}