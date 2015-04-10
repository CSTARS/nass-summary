# nass-summary

This project summarizes the USDA NASS Statisitics into a database summary used for crop budgets.  Basically, this combines a set of NASS data runs into a set of summary tables that we use a lot.


## How to run quickstats

One unfortunate part of the process is getting the data in the first place.  There is no convenient method for downloading the data en mass, and you need to go through [Quick Stats 2.0](http://quickstats.nass.usda.gov/) to get the data.  You can't get everything at once, because the query is too large.  Here are some tips about getting the data you want.

* Work from the top down.  It's easier to change the lower values, and resubmit then the upper ones, so try and get to the point you are getting all the data you need, but only changing the year. 


### CENSUS DATA

There is a lot of CENSUS data, so 

* It seems like a good idea to get everything, so you only need to go here once, but it's better to limit the categories to things that you really need.  For example, if you only need acres and yields, you can get much more data by specifing these data items.  
  * For example, for yields, you can select All COMMODITIES, and then select Data ITEMS; Area Harvested, Area in Production, Sales, Water Applied and Yield.  From that point you can get CA, ID,OR,WA,MT at about the per year basis.




### Survey Data

Survey Data is much smaller than census data, so you can take bigger chunks.  The best way to do this is select ALL survey data, then ALL the counties you want, then select years until you get up to 50K or you get all your data.  Name these : ```s_county_2007-2015.csv```, for example.

Make sure and get the Ag District and State data as well.  


