--Drop schema if exists quickstats cascade;
--create schema quickstats;
set search_path=quickstats,public;

create table usda_api (
year text,
commodity_desc text,
statisticcat_desc text,
county_code text,
source_desc text,
unit_desc text,
prodn_practice_desc text,
freq_desc text,
domain_desc text,
util_practice_desc text,
value text,
reference_period_desc text,
class_desc text,
asd_code text,
agg_level_desc text,
domaincat_desc text,
state_fips_code text,
state_alpha text,
group_desc text);

create table usda_region (
state_alpha text,
state_fips_code text,
county_code text,
county_name text,
asd_code text,
asd_name text);


create table quickstats_raw (
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


create table quickstats as 
select * from quickstats_raw limit 0;


create or replace function updateQuickStats() RETURNS bigint
AS $$
update quickstats_raw
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

select count(*) from quickstats_raw;
$$ LANGUAGE SQL;

create or replace function makeQuickStats() RETURNS bigint
AS $$
select * from updateQuickStats();
truncate quickstats.quickstats;
insert into quickstats.quickstats
select distinct * from quickstats.quickstats_raw;
select count(*) from quickstats.quickstats;
$$ LANGUAGE SQL;


--select "Value"::float from quickstats where "Value" not in (' (NA)',' (D)','',' (S)');
