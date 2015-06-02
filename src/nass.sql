drop schema nass cascade;
create schema nass;
set search_path=nass,public;

-- Now we start to build our tables
-- Crosswalk between counties and ag_districts
create view county_adc as 
select distinct 
statefips||countycode as fips,
statefips||'ag'||agdistrictcode as adc,
state,agdistrict,county 
from quickstats.quickstats 
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
from quickstats.quickstats 
where dataitem~'RENT, CASH,.*' 
order 
by location,year,dataitem,value;

create materialized view stats_location as
with a as (
 select q.*,
 CASE WHEN (countycode != '') THEN statefips||countycode 
      WHEN (agdistrictcode != '') THEN statefips||'ag'||agdistrictcode 
      ELSE statefips END as location,
 to_number(value,'9999999999D99') as value_number,
 string_to_array(dataitem,' - ') as di
 from quickstats.quickstats q
 where period='YEAR' and
 domain='TOTAL' and program='CENSUS' and
 not value~'^\(.*\)'
)
select distinct
commodity,location,year,value_number as value,
--string_to_array(regexp_replace(di[1],commodity||'(, )?',''),', ') as commodity_a,
string_to_array(di[1],', ') as commodity_a,
string_to_array(di[2],', ') as item_a,
di[2] as item,
dataitem
from a;

-- ACRES  Harvested
create or replace view acres_location as
select commodity,location,year,value as acres,
array_remove(commodity_a,commodity) as subcommodity,
item_a
from stats_location 
where item_a[1] in ('ACRES HARVESTED','ACRES BEARING','ACRES IN PRODUCTION');

create or replace view production_location as
select commodity,location,year,value as production,
regexp_replace(item_a[2],'MEASURED IN ','') as unit,
array_remove(commodity_a,commodity) as subcommodity,
item_a[3:10] as subproduction
from stats_location 
where item_a[1] in ('PRODUCTION');

create or replace view harvest_location as
with h as (
select commodity,location,year,subcommodity,max(acres) as acres
from acres_location 
group by commodity,location,year,subcommodity
)
select commodity,location,year,subcommodity,
acres,production,unit
from production_location p 
full outer join h using (commodity,location,year,subcommodity);

-- CENSUS data does NOT have NON-IRRIGATED however
create view subcommodity_explicitly_irrigated as 
 select distinct commodity,
 array_remove(subcommodity,'IRRIGATED') as subcommodity
 from harvest_location
 where 'IRRIGATED'=ANY (subcommodity);

create or replace view commodity_explicitly_irrigated as 
select distinct commodity
from subcommodity_explicitly_irrigated;

--\COPY (select * from commodity_explicit_irrigation order by 1) to commodity_explicit_irrigation.csv with csv header

-- There are not PRODUCTION estimates for 'IRRIGATED' subcommodities
create view harvest_location_irrigated as 
with i as (
select 
commodity,location,year,acres,production,unit,
array_remove(subcommodity,'IRRIGATED') as subcommodity
from harvest_location 
where 'IRRIGATED' = ANY (subcommodity)
),
n as (
select 
commodity,location,year,acres,production,unit,
subcommodity
from harvest_location 
where not 'IRRIGATED'= ANY (subcommodity)
)
select 
commodity,location,year,subcommodity,
s is not null as explicitly_irrigated,
i.acres as irrigated_acres,
coalesce(n.acres,i.acres) as total_acres,
n.production as total_production,
n.unit
from 
n full outer join i using (commodity,location,year,subcommodity) 
left join subcommodity_explicitly_irrigated s using (commodity,subcommodity);

-- This shows the sums of NASS from leaves alone
create view harvest_by_leaves as 
with recursive b(commodity,location,year,subcommodity,irrigated_acres,total_acres,total_production,unit) 
as (
select
commodity,location,year,
subcommodity,
irrigated_acres,total_acres, 
total_production,unit
from harvest_location_irrigated 
left join (
 select commodity,location,year,
 subcommodity[1:array_length(subcommodity,1)-1] as subcommodity 
from harvest_location_irrigated ) as r
using (commodity,location,year,subcommodity) 
where r is null
union
select commodity,location,year,
subcommodity[1:array_length(subcommodity,1)-1] as subcommodity,
sum(irrigated_acres) over W as irrigated_acres,
sum(total_acres) over W as total_acres,
sum(total_production) over W as total_production,unit
from b where 
array_length(subcommodity,1)>0
WINDOW W as (partition by commodity,location,year,
             subcommodity[1:array_length(subcommodity,1)-1])
) 
select commodity,location,year,subcommodity,
--irrigated_acres,total_acres,
--total_production
sum(irrigated_acres) as irrigated_acres,sum(total_acres) as total_acres,
sum(total_production) as total_production,unit
from b
group by commodity,location,year,subcommodity,unit;

-- This is an attempt to capture the total amount of harvest from
-- the nass statistics for each location and commodity.  It creates
-- the missing tree values, but uses them when they exist.

create materialized view harvest_total_and_sum as 
select commodity,location,year,subcommodity,
(t.commodity is not null) as reported,
t.irrigated_acres as t_irrigated_acres,t.total_acres as t_total_acres,
s.irrigated_acres as s_irrigated_acres,s.total_acres as s_total_acres,
greatest(t.irrigated_acres,s.irrigated_acres) as irrigated_acres,
greatest(t.total_acres,s.total_acres) as total_acres,
t.total_production as t_total_production,
s.total_production as s_total_production,
greatest(t.total_production,s.total_production) as total_production,
unit
from harvest_location_irrigated t full outer join
harvest_by_leaves s using (commodity,location,year,subcommodity,unit)
order by year,location,commodity,subcommodity;

create view commodity_harvest as 
select 
array_to_string(array_prepend(commodity,subcommodity),', ') as commodity,
location,year,reported,
irrigated_acres,total_acres,total_production,unit
from nass.harvest_total_and_sum 
order by year,location,commodity;

create view commodity_harvest_list as 
select distinct commodity from commodity_harvest
order by commodity;


-- Yield is similar to harvests in that we need to summarize and
-- aggregate the data.

-- create or replace view yield_location as
-- with a as (
--  select q.*,
--  CASE WHEN (countycode != '') THEN statefips||countycode 
--       WHEN (agdistrictcode != '') THEN statefips||'ag'||agdistrictcode 
--       ELSE statefips END as location,
--  to_number(value,'999999.99') as yield,
--  string_to_array(dataitem,' - ') as di
--  from quickstats.quickstats q
--  where domain='TOTAL' and program='SURVEY' and
--  dataitem ~ ' - YIELD' and
--  not value~'^\(.*\)'
-- )
-- select 
-- commodity,location,year,yield,
-- string_to_array(regexp_replace(di[1],commodity||'(, )?',''),', ') 
--   as subcommodity,
-- 'YIELD'::text as item,
-- regexp_replace(di[2],'^YIELD, MEASURED IN ','') as unit
-- from a;

create or replace view yield_location as
select 
commodity,location,year,value as yield,
regexp_replace(item_a[2],'MEASURED IN ','') as unit,
array_remove(commodity_a,commodity) as subcommodity,
item_a[3:10] as subyield
from stats_location where item_a[1] in ('YIELD');


create materialized view yield_location_irrigated as 
with i as (
select 
commodity,location,year,yield,
array_remove(array_remove(subcommodity,'IRRIGATED'),'ENTIRE CROP') as subcommodity,
unit
from yield_location 
where 'IRRIGATED' = ANY (subcommodity) and
'ENTIRE CROP' = ANY(subcommodity)
),
p as (
select 
commodity,location,year,yield,
array_remove(array_remove(subcommodity,'IRRIGATED'),'PART OF CROP') as subcommodity,
unit
from yield_location 
where 'IRRIGATED' = ANY (subcommodity) and
'PART OF CROP' = ANY(subcommodity)
),
n as (
select 
commodity,location,year,yield,
array_remove(array_remove(subcommodity,'IRRIGATED'),'NONE OF CROP') as subcommodity,
unit
from yield_location 
where 'IRRIGATED'= ANY (subcommodity) and
'NONE OF CROP' = ANY(subcommodity)
),
u as (
select 
commodity,location,year,yield,
subcommodity,
unit
from yield_location 
where not 'IRRIGATED'= ANY (subcommodity)
)
select 
commodity,location,year,subcommodity,unit,
i.yield as irrigated,
p.yield as partial,
n.yield as none,
u.yield as unspecified
from 
i full outer join p using (commodity,location,year,subcommodity,unit)
full outer join n using (commodity,location,year,subcommodity,unit)
full outer join u using (commodity,location,year,subcommodity,unit);


-- This shows the sums of NASS from leaves alone
create view yield_by_leaves as 
with recursive b(commodity,location,year,subcommodity,unit,irrigated,
                 partial,none,unspecified) 
as (
select
commodity,location,year,
subcommodity,unit,
irrigated,partial,none,unspecified
from yield_location_irrigated 
left join (
 select commodity,location,year,
 subcommodity[1:array_length(subcommodity,1)-1] as subcommodity,
 unit
from yield_location_irrigated ) as r
using (commodity,location,year,subcommodity,unit) 
where r is null
union
select commodity,location,year,
subcommodity[1:array_length(subcommodity,1)-1] as subcommodity,
unit,
avg(irrigated) over W as irrigated,
avg(partial) over W as partial,
avg(none) over W as none,
avg(unspecified) over W as unspecified
from b where 
array_length(subcommodity,1)>0
WINDOW W as (partition by commodity,location,year,
             subcommodity[1:array_length(subcommodity,1)-1],unit)
) 
select commodity,location,year,subcommodity,unit,
avg(irrigated)::decimal(8,2) as irrigated,
avg(partial)::decimal(8,2) as partial,
avg(none)::decimal(8,2) as none,
avg(unspecified)::decimal(8,2) as unspecified
from b
group by commodity,location,year,subcommodity,unit;


create materialized view yield_total_and_sum as 
select commodity,location,year,subcommodity,unit,
t.irrigated as t_irrigated,
t.partial as t_partial,
t.none as t_none,
t.unspecified as t_unspecified,
s.irrigated as s_irrigated,
s.none as s_none,
s.partial as s_partial,
s.unspecified as s_unspecified,
coalesce(t.irrigated,s.irrigated) as irrigated,
coalesce(t.none,s.none) as none,
coalesce(t.partial,s.partial) as partial,
coalesce(t.unspecified,s.unspecified) as unspecified
from yield_location_irrigated t full outer join
yield_by_leaves s using (commodity,location,year,subcommodity,unit)
order by year,location,commodity,subcommodity,unit;

create view commodity_yield as 
select 
array_to_string(array_prepend(commodity,subcommodity),', ') as commodity,
location,year,unit,irrigated,partial,none,unspecified 
from nass.yield_total_and_sum 
order by year,location,commodity,unit;

-- Prices are aggreated, but we don't need to worry about irrigation.

create or replace view price_location as
with a as (
 select q.*,
 CASE WHEN (countycode != '') THEN statefips||countycode 
      WHEN (agdistrictcode != '') THEN statefips||'ag'||agdistrictcode 
      ELSE statefips END as location,
 to_number(value,'9999.99') as price,
 string_to_array(dataitem,' - ') as di
 from quickstats.quickstats q
 where domain='TOTAL' and program='SURVEY' and period='YEAR' and
 dataitem ~ ' - PRICE RECEIVED' and
 not value~'^\(.*\)'
)
select distinct
commodity,location,year,price,
string_to_array(regexp_replace(di[1],commodity||'(, )?',''),', ') 
  as subcommodity,
'price'::text as item,
regexp_replace(di[2],'^PRICE RECEIVED, MEASURED IN ','') as unit
from a;

-- This shows the sums of NASS from leaves alone
create or replace view price_by_leaves as 
with recursive b(commodity,location,year,subcommodity,unit,price) 
as (
select
commodity,location,year,
subcommodity,unit,price
from price_location
left join (
 select commodity,location,year,
 subcommodity[1:array_length(subcommodity,1)-1] as subcommodity,
 unit
from price_location ) as r
using (commodity,location,year,subcommodity,unit) 
where r is null
union
select commodity,location,year,
subcommodity[1:array_length(subcommodity,1)-1] as subcommodity,
unit,
avg(price) over W
from b where 
array_length(subcommodity,1)>0
WINDOW W as (partition by commodity,location,year,
             subcommodity[1:array_length(subcommodity,1)-1],unit)
) 
select commodity,location,year,subcommodity,unit,
avg(price)::decimal(8,2) as price
from b
group by commodity,location,year,subcommodity,unit;

create or replace view price_total_and_sum as 
select commodity,location,year,subcommodity,unit,
t.price as t_price,s.price as s_price,
coalesce(t.price,s.price) as price
from price_location t full outer join
price_by_leaves s using (commodity,location,year,subcommodity,unit)
order by year,location,commodity,subcommodity,unit;

create view commodity_price as 
select 
array_to_string(array_prepend(commodity,subcommodity),', ') as commodity,
location,year,unit,price
from nass.price_total_and_sum 
order by year,location,commodity,unit;

create view commodity_list as 
with h as (select distinct commodity from commodity_harvest),
y as (select distinct commodity from commodity_yield),
p as (select distinct commodity from commodity_price)
select 
coalesce(h.commodity,y.commodity,p.commodity) as commodity,
h is not null as harvest,
y is not null as yield,
p is not null as price
from h full outer join y using (commodity)
full outer join p using (commodity)
order by commodity;

create or replace view commodity_county_yield as 
with f as (
 select distinct commodity,fips,adc,substr(fips,1,2) as state
 from commodity_harvest
 join county_adc  on (location=fips) 
 where location~'.....'
),
u as (
 select distinct commodity,unit 
 from commodity_harvest where unit is not null
), 
c as (
 select commodity,location as fips,
 avg((total_production/total_acres))::decimal(10,2) as yield,
 unit
 from commodity_harvest 
 where total_acres is not null 
 and total_production is not null and 
 location~'.....' 
 group by commodity,location,unit
), 
a as (
 select commodity,
 adc,
 avg(yield)::decimal(10,2) as yield,unit
 from c join county_adc using (fips) 
 group by commodity,adc,unit 
 order by commodity,adc,unit
), 
s as (
 select commodity,
 substr(fips,1,2) as state,
 avg(yield)::decimal(10,2) as yield,
 unit 
 from c 
 group by commodity,substr(fips,1,2),unit
),
sy as (
 select commodity,
 regexp_replace(unit,' / ACRE','') as unit,
 location as state,
 avg(irrigated)::decimal(10,2) as irrigated,
 avg(partial)::decimal(10,2) as partial,
 avg(none)::decimal(10,2) as none,
 avg(unspecified)::decimal(10,2) as unspecified 
from commodity_yield 
group by commodity,unit,location
),
al as (
select commodity,fips,adc,state,unit,
c.yield as county_yield,
a.yield as ad_yield,
s.yield as st_yield
from f join u using (commodity)
left join c using (commodity,fips,unit) 
left join a using (commodity,adc,unit)
left join s using (commodity,state,unit)
)
select 
commodity,unit,fips,adc,state,
coalesce(county_yield,ad_yield,st_yield,partial) as yield,
county_yield,ad_yield,st_yield,
irrigated as st_irrigated,
partial as st_partial,
none as st_none
from al full outer join sy 
using (commodity,state,unit)
order by commodity;
