set search_path=nass,public;

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

create materialized view subcommodity_explicit_irrigation as 
with i as (
select distinct
commodity,
subcommodity[1:array_length(subcommodity,1)-1] as subcommodity
from location_harvested 
where subcommodity[array_length(subcommodity,1)]='IRRIGATED'
),
n as (
select distinct
commodity,
subcommodity
from location_harvested 
where array_length(subcommodity,1) is null or 
subcommodity[array_length(subcommodity,1)]!='IRRIGATED'
)
select 
commodity,
subcommodity,
i is not null as irrigated
from n left join i using (commodity,subcommodity);

create or replace view commodity_explicit_irrigation as 
select commodity,bool_or(irrigated) as irrigation
from subcommodity_explicit_irrigation group by commodity;

--\COPY (select * from commodity_explicit_irrigation order by 1) to commodity_explicit_irrigation.csv with csv header

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
),
n as (
select 
commodity,location,year,irrigated,total
from r where max=0 and sub_len=0
union 
select 
commodity,location,year,
sum(irrigated) as irrigated,
sum(total) as total
from r where max !=0 and sub_len=1
group by commodity,location,year
)
select commodity,location,year,
CASE WHEN (e.irrigation is false) THEN total ELSE coalesce(irrigated,0) END as irrigated,
total-(CASE WHEN (e.irrigation is false) THEN total ELSE coalesce(irrigated,0) END) as non_irr,
total as total
from n join commodity_explicit_irrigation e using (commodity);

--\COPY (select * from commodity_total_harvest order by 3,2,1) to commodity_total_harvest.csv with csv header;


