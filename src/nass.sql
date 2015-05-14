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

-- ACRES  Harvested
create or replace view harvest_location as
with a as (
 select q.*,
 CASE WHEN (countycode != '') THEN statefips||countycode 
      WHEN (agdistrictcode != '') THEN statefips||'ag'||agdistrictcode 
      ELSE statefips END as location,
 to_number(value,'999999') as acres,
 string_to_array(dataitem,' - ') as di
 from quickstats.quickstats q
 where domain='TOTAL' and program='CENSUS' and
-- countycode != '' and
 not value~'^\(.*\)'
)
select 
commodity,location,year,acres,
string_to_array(regexp_replace(di[1],commodity||'(, )?',''),', ') 
						    as subcommodity,
di[2] as item
from a
where
di[2]='ACRES HARVESTED';

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

create materialized view harvest_location_irrigated as 
with i as (
select 
commodity,location,year,acres,
array_remove(subcommodity,'IRRIGATED') as subcommodity
from harvest_location 
where 'IRRIGATED' = ANY (subcommodity)
),
n as (
select 
commodity,location,year,acres,
subcommodity
from harvest_location 
where not 'IRRIGATED'= ANY (subcommodity)
)
select 
commodity,location,year,subcommodity,
s is not null as explicitly_irrigated,
i.acres as irrigated,
coalesce(n.acres,i.acres) as total
from 
n full outer join i using (commodity,location,year,subcommodity) 
left join subcommodity_explicitly_irrigated s using (commodity,subcommodity);

-- This shows the sums of NASS from leaves alone
create view harvest_by_leaves as 
with recursive b(commodity,location,year,subcommodity,irrigated,total) 
as (
select
commodity,location,year,
subcommodity,irrigated,total 
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
sum(irrigated) over W as irrigated,
sum(total) over W as total
from b where 
array_length(subcommodity,1)>0
WINDOW W as (partition by commodity,location,year,
             subcommodity[1:array_length(subcommodity,1)-1])
) 
select commodity,location,year,subcommodity,
sum(irrigated) as irrigated,sum(total) as total
from b
group by commodity,location,year,subcommodity;

-- This is an attempt to capture the total amount of production from
-- the nass statistics for each location and commodity.  It creates
-- the missing tree values, but uses them when they exist.

create materialized view harvest_total_and_sum as 
select commodity,location,year,subcommodity,
t.irrigated as t_irrigated,t.total as t_total,
s.irrigated as s_irrigated,s.total as s_total,
greatest(t.irrigated,s.irrigated) as irrigated,
greatest(t.total,s.total) as total
from harvest_location_irrigated t full outer join
harvest_by_leaves s using (commodity,location,year,subcommodity)
order by year,location,commodity,subcommodity;

create view commodity_harvest as 
select commodity,location,year,
irrigated,total
from harvest_total_and_sum 
where subcommodity='{}';


-- Yield is similar to production in that we need to summarize and
-- aggregate the data.

create or replace view yield_location as
with a as (
 select q.*,
 CASE WHEN (countycode != '') THEN statefips||countycode 
      WHEN (agdistrictcode != '') THEN statefips||'ag'||agdistrictcode 
      ELSE statefips END as location,
 to_number(value,'999999.99') as yield,
 string_to_array(dataitem,' - ') as di
 from quickstats.quickstats q
 where domain='TOTAL' and program='SURVEY' and
 dataitem ~ ' - YIELD' and
 not value~'^\(.*\)'
)
select 
commodity,location,year,yield,
string_to_array(regexp_replace(di[1],commodity||'(, )?',''),', ') 
  as subcommodity,
'YIELD'::text as item,
regexp_replace(di[2],'^YIELD, MEASURED IN ','') as unit
from a;

-- SURVEY data does NON-IRRIGATED however
create view yield_explicitly_irrigated as 
with i as (
 select distinct commodity,
 array_remove(subcommodity,'IRRIGATED') as subcommodity,
 unit,
 true as irrigated
 from yield_location
 where 'IRRIGATED'=ANY (subcommodity)
),
n as (
 select distinct commodity,
 array_remove(subcommodity,'NON-IRRIGATED') as subcommodity,
 unit,
 true as non_irrigated
 from yield_location
 where 'NON-IRRIGATED'=ANY (subcommodity)
)
select commodity,subcommodity,unit,i.irrigated,n.non_irrigated
from i full outer join n using(commodity,subcommodity,unit);

create materialized view yield_location_irrigated as 
with i as (
select 
commodity,location,year,yield,
array_remove(subcommodity,'IRRIGATED') as subcommodity,
unit
from yield_location 
where 'IRRIGATED' = ANY (subcommodity)
),
n as (
select 
commodity,location,year,yield,
array_remove(subcommodity,'NON-IRRIGATED') as subcommodity,
unit
from yield_location 
where 'NON-IRRIGATED'= ANY (subcommodity)
),
u as (
select 
commodity,location,year,yield,
subcommodity,
unit
from yield_location 
where not 'IRRIGATED'= ANY (subcommodity) and
not 'NON-IRRIGATED'= ANY (subcommodity)
)
select 
commodity,location,year,subcommodity,unit,
coalesce(s.irrigated,false) as explicitly_irrigated,
coalesce(s.non_irrigated,false) as explicitly_non_irrigated,
i.yield as irrigated,
n.yield as non_irrigated,
u.yield as unspecified
from 
n full outer join i using (commodity,location,year,subcommodity,unit)
full outer join u using (commodity,location,year,subcommodity,unit)
left join yield_explicitly_irrigated s using (commodity,subcommodity,unit);

-- This shows the sums of NASS from leaves alone
create view yield_by_leaves as 
with recursive b(commodity,location,year,subcommodity,unit,irrigated,
                 non_irrigated,unspecified) 
as (
select
commodity,location,year,
subcommodity,unit,
irrigated,non_irrigated,unspecified
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
avg(non_irrigated) over W as non_irrigated,
avg(unspecified) over W as unspecified
from b where 
array_length(subcommodity,1)>0
WINDOW W as (partition by commodity,location,year,
             subcommodity[1:array_length(subcommodity,1)-1],unit)
) 
select commodity,location,year,subcommodity,unit,
avg(irrigated)::decimal(8,2) as irrigated,
avg(non_irrigated)::decimal(8,2) as non_irrigated,
avg(unspecified)::decimal(8,2) as unspecified
from b
group by commodity,location,year,subcommodity,unit;


create materialized view yield_total_and_sum as 
select commodity,location,year,subcommodity,unit,
t.irrigated as t_irrigated,
t.non_irrigated as t_non_irrigated,
t.unspecified as t_unspecified,
s.irrigated as s_irrigated,
s.non_irrigated as s_non_irrigated,
s.unspecified as s_unspecified,
coalesce(t.irrigated,s.irrigated) as irrigated,
coalesce(t.non_irrigated,s.non_irrigated) as non_irrigated,
coalesce(t.unspecified,s.unspecified) as unspecified
from yield_location_irrigated t full outer join
yield_by_leaves s using (commodity,location,year,subcommodity,unit)
order by year,location,commodity,subcommodity,unit;

create view commodity_yield as 
select commodity,location,year,unit,
irrigated,non_irrigated,unspecified
from yield_total_and_sum 
where subcommodity='{}';

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
select commodity,location,year,unit,price
from price_total_and_sum 
where subcommodity='{}';
