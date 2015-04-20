Drop schema if exists nass cascade;
create schema nass;
set search_path=nass,public;

-- Quick stats puts all data in one standard table.  Survey and Census
create table quickstats_old (
Program text,
Year integer,
Period text,
WeekEnding text,
GeoLevel text,
State text,
StateFips char(2),
AgDistrict text,
AgDistrictCode text,
County text,
CountyCode char(5),
ZipCode varchar(5),
Region text,
Watershed text,
DataItem text,
Domain text,
DomainCategory text,
Value text
);

create table quickstats (
Program text,
Year integer,
Period text,
WeekEnding text,
GeoLevel text,
State text,
StateFips char(2),
AgDistrict text,
AgDistrictCode text,
County text,
CountyCode char(5),
ZipCode varchar(5),
Region text,
WatershedCode text,
Watershed text,
Commodity text,
DataItem text,
Domain text,
DomainCategory text,
Value text,
CV Text
);


create or replace function updateQuickStats() RETURNS bigint
AS $$
update quickstats 
set Program=trim(both from Program),
Period=trim(both from Period),
WeekEnding=trim(both from WeekEnding),
GeoLevel=trim(both from GeoLevel),
State=trim(both from State),
StateFips=trim(both from StateFips),
AgDistrict=trim(both from AgDistrict),
AgDistrictCode=trim(both from AgDistrictCode),
County=trim(both from County),
CountyCode=trim(both from CountyCode),
ZipCode=trim(both from ZipCode),
Region=trim(both from Region),
WatershedCode=trim(both from WatershedCode),
Watershed=trim(both from Watershed),
Commodity=trim(both from Commodity),
DataItem=trim(both from DataItem),
Domain=trim(both from Domain),
DomainCategory=trim(both from DomainCategory),
Value=trim(both from Value),
CV=trim(both from CV);

select count(*) from quickstats;
$$ LANGUAGE SQL;

--select "Value"::float from quickstats where "Value" not in (' (NA)',' (D)','',' (S)');

-- Now we start to build our tables
-- Crosswalk between counties and ag_districts
create view county_adc as 
select distinct 
statefips||countycode as fips,
statefips||'ag'||agdistrictcode as adc,
state,agdistrict,county 
from quickstats 
where countycode !='' and agdistrictcode!='';

-- Land Rent (From Survey data)
create view land_rent as
select 
CASE WHEN (countycode != '') THEN statefips||countycode 
     WHEN (agdistrictcode != '') THEN statefips||'ag'||agdistrictcode 
     ELSE statefips END as location,
year,
(dataitem~' IRRIGATED') as irrigated,
(dataitem~'NON-IRRIGATED') as non_irrigated,
(dataitem~'PASTURE') as pasture,
to_number(value,'99999D99') as value,
dataitem 
from quickstats 
where dataitem~'RENT, CASH,.*' 
order 
by location,year,dataitem,value;

-- ACRES  Harvested
create or replace view location_harvested as
with a as (
 select q.*,
 CASE WHEN (countycode != '') THEN statefips||countycode 
      WHEN (agdistrictcode != '') THEN statefips||'ag'||agdistrictcode 
      ELSE statefips END as location,
 to_number(value,'999999') as acres,
 string_to_array(dataitem,' - ') as di
 from quickstats q
 where domain='TOTAL' and program='CENSUS' and
 countycode != '' and
 not value~'^\(.*\)'
)
select 
commodity,
location,
year,
acres,
string_to_array(regexp_replace(di[1],commodity||'(, )?',''),', ') as subcommodity,
di[2] as item
from a
where
di[2]='ACRES HARVESTED';

create materialized view commodity_irrigated as 
with i as (
select 
commodity,
location,
year,
acres,
subcommodity[1:array_length(subcommodity,1)-1] as subcommodity
from location_harvested 
where subcommodity[array_length(subcommodity,1)]='IRRIGATED'
),
n as (
select 
commodity,
location,
year,
acres,
subcommodity
from location_harvested 
where array_length(subcommodity,1) is null or 
subcommodity[array_length(subcommodity,1)]!='IRRIGATED'
)
select 
commodity,
location,
year,
subcommodity,
coalesce(array_length(subcommodity,1),0) as sub_len,
i.acres as irrigated,n.acres as total
from 
n left join i using (commodity,location,year,subcommodity);

create materialized view commodity_total_harvest as 
with r as (
select *,
min(sub_len) OVER W as min,
max(sub_len) OVER W as max
from commodity_irrigated
WINDOW W as (partition by commodity,location,year)
)
select 
commodity,location,year,irrigated,total
from r where max=0 and sub_len=0
union 
select 
commodity,location,year,
sum(irrigated) as irrigated,
sum(total) as total
from r where max !=0 and sub_len=1
group by commodity,location,year;


