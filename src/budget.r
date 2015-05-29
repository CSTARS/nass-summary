## Budget 
## Download area harvested from NASS quick stat
## Yu Pei
## May-2015 
setwd('~/Documents/GSR/budget/')

yukey = "YOUR API KEY"
states = c('06', '16', '30', '41', '53')
## Specify What, Where, and When
getArea = function(mykey = yukey, year= 2005, state = states[1], verbose = FALSE){
  
  # Grab all the data in one state, in one year
  res = read.csv(paste0('http://quickstats.nass.usda.gov/api/api_GET/?key=',
                        mykey, 
                        '&year=',year,'&sector_desc=CROPS','&unit_desc=ACRES',
                        '&state_fips_code=', state,
                        '&statisticcat_desc=AREA%20HARVESTED','&freq_desc=ANNUAL',
                        '&format=CSV')
                ,stringsAsFactors = F)
  
  if(verbose)print('Completed downloading data.')
  
  # Get rid of useless columns eg country etc
  res = res[, c("source_desc", "commodity_desc", "short_desc", 
                "domain_desc", "domaincat_desc","agg_level_desc",
                "state_fips_code", "asd_code", "county_code",
                "year", "Value", 'reference_period_desc')]
  
  # Filter to domain_desc to be TOTAL
  res = res[res$domain_desc == 'TOTAL' & res$reference_period_desc == 'YEAR' &
              res$agg_level_desc %in% c('AGRICULTURAL DISTRICT', 'COUNTY', 'STATE'), ]
  
  ## Removing irrigation breakdown, just keep total and irrigated.
  # "BARLEY, IRRIGATED"                                 
  #"BARLEY, IRRIGATED, ENTIRE CROP"                     
  #"BARLEY, IRRIGATED, NONE OF CROP" "BARLEY, IRRIGATED, PART OF CROP, IRRIGATED PORTION"
  #"BARLEY, NON-IRRIGATED" 
  xx = grepl(', NON-IRRIGATED', res$short_desc)
  res = res[!xx, ]
  xx = grepl(', IRRIGATED, ENTIRE CROP', res$short_desc)
  res = res[!xx, ]
  xx = grepl(', IRRIGATED, NONE OF CROP', res$short_desc)
  res = res[!xx, ]
  xx = grepl(', PART OF CROP, IRRIGATED PORTION', res$short_desc)
  res = res[!xx, ]
  
  # Get crop names 
  crops = gsub('(.*) - ACRES.*', '\\1', res$short_desc)
  res$crops = crops
  # Convert Value to numbers
  res$Value = suppressWarnings(as.numeric(gsub(',', '', res$Value)))
  
  # Add in location column formatC to pad 0
  res$state_fips_code = formatC(res$state_fips_code, width = 2, format = 'd', flag = '0')
  res$county_code = formatC(res$county_code, width = 3, format = 'd', flag = '0')
  res$asd_code = formatC(res$asd_code, width = 2, format = 'd', flag = '0')
  
  ## Remove 998(combinded county)
  res = res[res$county_code != 998, ]
  
  if(verbose)print('Finished Preprocessing, final step')
  res$location = ifelse(res$agg_level_desc == 'STATE', res$state_fips_code, 
                        ifelse(res$agg_level_desc == 'COUNTY', 
                               paste0(res$state_fips_code,res$county_code), 
                               paste0(res$state_fips_code,"ag",res$asd_code)))
  
  # Remove redundant columns 
  res = res[, c("source_desc","year", 'crops', 'location' , "Value")]
  names(res) = c("source_desc","year", 'crops', 'location' , "total")
  ## TODO split out irrigated part, create new column
  idx = grepl(', IRRIGATED', res$crops)
  irrigated = res[idx ,]
  res = res[!idx , ]
  # Change irrigated table, ready to join back to main table(res)
  irrigated = irrigated[, c("source_desc",'crops', 'location', 'total')]
  names(irrigated) = c('source_desc','crops', 'location', 'irrigated')
  irrigated$crops = gsub(', IRRIGATED', '', irrigated$crops)
  res = merge(res, irrigated, all.x = TRUE)
  
  # If contain CENSUS data, only take census data
  # First split on crops, then within crop, split on location
  temp = split(res, res$crops)
  temp2 = lapply(temp, function(onecrop){
    aa = split(onecrop, onecrop$location) # Split one crop based on location
    a = lapply(aa, function(x)x[1,]) # Take first row (CENSUS if have two row)
    do.call(rbind, a) #return result
  })
  dat = do.call(rbind, temp2)
  row.names(dat) = 1:nrow(dat)
  

 dat # Return
}


### Wrapper to get all year and all state data
getall = function(curyear = 2005){
  s06 = getArea(year = curyear,state = '06')
  s16 = getArea(year = curyear,state = '16')
  s30 = getArea(year = curyear,state = '30')
  s41 = getArea(year = curyear,state = '41')
  s53 = getArea(year = curyear,state = '53')
  
  dat = rbind(s06, s16,s30, s41, s53)
  dat
}

for(yr in 2005:2014){
  dat = getall(yr)
  write.csv(dat, paste0('./output/harvest',yr, '.csv'), row.names =F)
  print(paste('finished', yr))
}








######### Under construction








aa = getURL('http://quickstats.nass.usda.gov/api/get_param_values/?key=BBD12288-8892-3A35-8F67-4AC2E884391E&param=commodity_desc')

## SURVEY vs CENSUS

## Combine rows based on domaincat_desc


## Get yields
res = read.csv(paste0('http://quickstats.nass.usda.gov/api/api_GET/?key=',
                      mykey, 
                      '&year=',2007,'&sector_desc=CROPS',
                      '&state_fips_code=', '41',
                      '&statisticcat_desc=PRICE%20RECEIVED','&freq_desc=ANNUAL',
                      '&format=CSV')
               ,stringsAsFactors = F)
res[, c(1,4,9,10,38)]
