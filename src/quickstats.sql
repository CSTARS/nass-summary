--Drop schema if exists quickstats cascade;
--create schema quickstats;
set search_path=quickstats,public;

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
