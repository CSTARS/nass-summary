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
select
commodity, 
CASE WHEN (countycode != '') THEN statefips||countycode 
     WHEN (agdistrictcode != '') THEN statefips||'ag'||agdistrictcode 
     ELSE statefips END as location,
year,
to_number(value,'999999') as acres,
regexp_replace(dataitem,'^'||commodity||'(\s*[,-]\s*(.*))?\s+-\s+[^-]+$','\2') as subcommodity,
regexp_replace(dataitem,'.*-\s*([^-]+)$','\1') as item,
dataitem
from quickstats 
where domain='TOTAL' and 
countycode != '' and
dataitem like '%ACRES HARVESTED' and 
not value~'^\(.*\)';
