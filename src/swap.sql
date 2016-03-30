drop schema swap cascade;
create schema swap;
set search_path=swap,nass,public;

create table counties (
location varchar(12)
);
insert into counties select unnest(
ARRAY[
'06001','06003','06005','06007','06009','06011','06013','06015','06017','06019','06021','06023','06033','06035','06039','06041','06043','06045','06047','06049','06051','06055','06057','06061','06063','06067','06075','06077','06081','06089','06091','06093','06095','06097','06099','06101','06103','06105','06109','06113','06115','16001','16003','16009','16013','16015','16017','16021','16023','16025','16027','16031','16035','16037','16039','16045','16047','16049','16053','16055','16057','16059','16061','16063','16067','16069','16073','16075','16079','16083','16085','16087','30001','30023','30029','30035','30039','30047','30049','30053','30061','30063','30073','30077','30081','30089','30099','32001','32005','32007','32009','32011','32013','32015','32017','32019','32021','32023','32027','32029','32031','32033','32510','41001','41003','41005','41007','41009','41011','41013','41015','41017','41019','41021','41023','41025','41027','41029','41031','41033','41035','41037','41039','41041','41043','41045','41047','41049','41051','41053','41055','41057','41059','41061','41063','41065','41067','41069','41071','49001','49003','49021','49023','49027','49045','53000','53001','53003','53005','53007','53009','53011','53013','53015','53017','53019','53021','53023','53025','53027','53029','53031','53033','53035','53037','53039','53041','53043','53045','53047','53049','53051','53053','53055','53057','53059','53061','53063','53065','53067','53069','53071','53073','53075','53077']);

create table commodity (
commodity text);

insert into commodity select unnest( 
ARRAY['HAY, ALFALFA','HAYLAGE, ALFALFA','BARLEY',
'BEANS, DRY EDIBLE','BEANS, DRY EDIBLE, LIMA',
'CORN, GRAIN','CORN, SILAGE','HAY, TAME, (EXCL ALFALFA & SMALL GRAIN)',
'HAYLAGE, (EXCL ALFALFA)','LENTILS','OATS','POPLAR','POTATOES',
'WHEAT, SPRING','SUGARBEETS','WHEAT, WINTER']);

create or replace view commodity_price as 
select 
 state_fips_code as location,year,
 commodity_desc||
 CASE WHEN (class_desc='ALL CLASSES') THEN '' 
 ELSE ', '||class_desc END ||
 CASE WHEN (util_practice_desc='ALL UTILIZATION PRACTICES') THEN '' 
 ELSE ', '||util_practice_desc END as commodity,
 to_number(value,'99999D99')::decimal(10,2) as price,
 unit_desc as unit 
from quickstats.usda_api 
where statisticcat_desc='PRICE RECEIVED' and 
domain_desc='TOTAL' and 
prodn_practice_desc='ALL PRODUCTION PRACTICES' and 
freq_desc='ANNUAL' and 
agg_level_desc='STATE' 
and not value ~ '\(.*\)'
order by 1,2;

create or replace view commodity_avg_price as 
select 
 location,commodity,avg(price)::decimal(10,2) as price,
 unit
from commodity_price
group by location,commodity,unit
order by 1,2;

create view commodity_missing_price as 
select 
-- 1 Ton silage for 5 BU or  $/BU = 0.2 *5 ( $/BU * BU/TON) http://cdp.wisc.edu/jenny/crop/estimating.pdf
location,'CORN, SILAGE' as commodity,0.2*5*price as price,'$ / TON' as unit 
from commodity_avg_price where commodity='CORN, GRAIN' and unit='$ / BU'
union
select
location,'HAYLAGE, ALFALFA',0.2*price,unit 
from commodity_avg_price where commodity='HAY, ALFALFA'
union
select
location,'HAYLAGE, (EXCL ALFALFA)',0.2*price,unit 
from commodity_avg_price where commodity='HAY, (EXCL ALFALFA)'
union
select
location,'HAY, TAME, (EXCL ALFALFA & SMALL GRAIN)',price,unit 
from commodity_avg_price where commodity='HAY'
union
select
location,'LENTILS',(price/100)::decimal(10,3),'$ / LB' 
from commodity_avg_price where commodity='LENTILS' and unit='$ / CWT'
union
select
'41','LENTILS',(price/100)::decimal(10,3),'$ / LB' 
from commodity_avg_price 
where commodity='LENTILS' and unit='$ / CWT' and location='16'
union
select
location,'BEANS, DRY EDIBLE, LIMA',price,unit 
from commodity_avg_price where commodity='BEANS, DRY EDIBLE'
union
select
location,'BEANS, DRY EDIBLE, LIMA',(price/100)::decimal(10,3),'$ / LB' 
from commodity_avg_price where commodity='BEANS, DRY EDIBLE' and unit='$ / CWT'
union
select
location,'BEANS, DRY EDIBLE',(price/100)::decimal(10,3),'$ / LB' 
from commodity_avg_price where commodity='BEANS, DRY EDIBLE' and unit='$ / CWT';


create view harvest_2012 as 
select 
distinct location,commodity,irrigated_acres,total_acres
from nass.commodity_harvest 
join commodity using (commodity) 
where year=2012 and location~'^\d{5}$' 
and total_acres is not null
order by 1,2;


create or replace view yield_avg_by_type as 
select commodity,location,unit,
avg(nonirrigated)::decimal(10,2) as nonirrigated, 
avg(irrigated)::decimal(10,2) as irrigated, 
avg(total)::decimal(10,2) as total,
count(*) 
from nass.yield_by_type 
where unit ~ '/ ACRE'
and (not commodity ~ '^BEANS' or unit !='CWT / ACRE')
group by 1,2,3 
union
select commodity,location,'LB / ACRE',
100*avg(nonirrigated)::decimal(10,2) as nonirrigated, 
100*avg(irrigated)::decimal(10,2) as irrigated, 
100*avg(total)::decimal(10,2) as total,
count(*) 
from nass.yield_by_type 
where unit ='CWT / ACRE' and commodity~'^BEANS'
group by 1,2,3 
order by 1,2,3; 

create view harvest_production_yield as
with h as (
select 
distinct location,commodity,irrigated_acres,
total_acres,total_production,unit
from nass.commodity_harvest 
join commodity using (commodity) 
where location~'^\d{5}$' 
and total_acres is not null
and total_production is not null
)
select 
location,commodity,
(avg(total_production/total_acres))::decimal(10,2) as production_yield,
unit||' / ACRE' as unit
from h
group by location,commodity,unit;


create view harvest_yield as
with u as (
select distinct commodity,unit 
from yield_avg_by_type
where unit ~ '/ ACRE$'
),
p as (
select * from swap.commodity_avg_price
union
select * from swap.commodity_missing_price
)
select
h.location,h.commodity,h.irrigated_acres,h.total_acres, 
u.unit,
coalesce(yc.nonirrigated,ya.nonirrigated,ys.nonirrigated) as nonirrigated,
coalesce(yc.irrigated,ya.irrigated,ys.irrigated) as irrigated,
coalesce(yc.total,ya.total,ys.total) as total,
coalesce(yc.location,ya.location,ys.location) as yield_source,
production_yield,
p.price,p.unit as price_unit
from harvest_2012 h join counties using (location)
join u using (commodity)
join county_adc adc on (h.location=adc.fips) 
left join yield_avg_by_type yc on (yc.location=adc.fips and h.commodity=yc.commodity and u.unit=yc.unit)
left join yield_avg_by_type ya on (ya.location=adc.asd and h.commodity=ya.commodity and  u.unit=ya.unit)
left join yield_avg_by_type ys on (ys.location=adc.state_fips and h.commodity=ys.commodity and u.unit=ys.unit)
left join harvest_production_yield yp on (yp.location=adc.fips and h.commodity=yp.commodity and u.unit=yp.unit)
left join p on (p.location=adc.state_fips and p.commodity=h.commodity
and (((string_to_array(u.unit,' / '))[1]=(string_to_array(p.unit,' / '))[2]) or
     (string_to_array(u.unit,' / '))[1]=(string_to_array(p.unit,' / '))[2]||'S'))
order by 1,2;

create view swap as 
select
location,commodity||', NON-IRRIGATED' as commodity,
(total_acres-irrigated_acres) as acres,
(CASE WHEN (production_yield is not null and 
 nonirrigated is not null and irrigated is not null and nonirrigated!=0)
THEN 
production_yield*total_acres/
(irrigated_acres*(irrigated/nonirrigated)+(total_acres-irrigated_acres))
ELSE
coalesce(nonirrigated,total)
END)::decimal(10,2) as yield,
price,
unit,price_unit
from harvest_yield where irrigated_acres is not null
union
select
location,commodity||', IRRIGATED' as commodity,
irrigated_acres as acres,
(CASE when (production_yield is not null and
 nonirrigated is not null and irrigated is not null)
THEN 
production_yield*total_acres/
(irrigated_acres+(total_acres-irrigated_acres)*(nonirrigated/irrigated))
ELSE
coalesce(irrigated,total)
END)::decimal(10,2) as yield,
price,
unit,price_unit
from harvest_yield where irrigated_acres is not null
union
select
location,commodity,
total_acres as acres,
(CASE WHEN (production_yield is not null)
THEN 
production_yield
ELSE
total
END)::decimal(10,2) as yield,
price,
unit,price_unit
from harvest_yield where irrigated_acres is null;

create view swap_net as
select * from crosstab(
'select location,commodity,acres from swap.swap order by 1,2'::text,
'select distinct commodity from swap.swap order by 1'::text
) as (
location varchar(12),
"BARLEY" decimal(10,2),
"BARLEY, IRRIGATED" decimal(10,2),
"BARLEY, NON-IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE" decimal(10,2),
"BEANS, DRY EDIBLE, IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE, LIMA" decimal(10,2),
"BEANS, DRY EDIBLE, LIMA, IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE, LIMA, NON-IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE, NON-IRRIGATED" decimal(10,2),
"CORN, GRAIN" decimal(10,2),
"CORN, GRAIN, IRRIGATED" decimal(10,2),
"CORN, GRAIN, NON-IRRIGATED" decimal(10,2),
"CORN, SILAGE" decimal(10,2),
"CORN, SILAGE, IRRIGATED" decimal(10,2),
"CORN, SILAGE, NON-IRRIGATED" decimal(10,2),
"HAY, ALFALFA" decimal(10,2),
"HAY, ALFALFA, IRRIGATED" decimal(10,2),
"HAY, ALFALFA, NON-IRRIGATED" decimal(10,2),
"HAYLAGE, ALFALFA" decimal(10,2),
"HAYLAGE, ALFALFA, IRRIGATED" decimal(10,2),
"HAYLAGE, ALFALFA, NON-IRRIGATED" decimal(10,2),
"HAYLAGE, (EXCL ALFALFA)" decimal(10,2),
"HAYLAGE, (EXCL ALFALFA), IRRIGATED" decimal(10,2),
"HAYLAGE, (EXCL ALFALFA), NON-IRRIGATED" decimal(10,2),
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN)" decimal(10,2),
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN), IRRIGATED" decimal(10,2),
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN), NON-IRRIGATED" decimal(10,2),
"LENTILS" decimal(10,2),
"LENTILS, IRRIGATED" decimal(10,2),
"LENTILS, NON-IRRIGATED" decimal(10,2),
"OATS" decimal(10,2),
"OATS, IRRIGATED" decimal(10,2),
"OATS, NON-IRRIGATED" decimal(10,2),
"POTATOES" decimal(10,2),
"SUGARBEETS, IRRIGATED" decimal(10,2),
"SUGARBEETS, NON-IRRIGATED" decimal(10,2),
"WHEAT, WINTER" decimal(10,2),
"WHEAT, WINTER, IRRIGATED" decimal(10,2),
"WHEAT, WINTER, NON-IRRIGATED" decimal(10,2));

create view swap_price as
select * from crosstab(
'select location,commodity,price from swap.swap order by 1,2'::text,
'select distinct commodity from swap.swap order by 1'::text
) as (
location varchar(12),
"BARLEY" decimal(10,2),
"BARLEY, IRRIGATED" decimal(10,2),
"BARLEY, NON-IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE" decimal(10,2),
"BEANS, DRY EDIBLE, IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE, LIMA" decimal(10,2),
"BEANS, DRY EDIBLE, LIMA, IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE, LIMA, NON-IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE, NON-IRRIGATED" decimal(10,2),
"CORN, GRAIN" decimal(10,2),
"CORN, GRAIN, IRRIGATED" decimal(10,2),
"CORN, GRAIN, NON-IRRIGATED" decimal(10,2),
"CORN, SILAGE" decimal(10,2),
"CORN, SILAGE, IRRIGATED" decimal(10,2),
"CORN, SILAGE, NON-IRRIGATED" decimal(10,2),
"HAY, ALFALFA" decimal(10,2),
"HAY, ALFALFA, IRRIGATED" decimal(10,2),
"HAY, ALFALFA, NON-IRRIGATED" decimal(10,2),
"HAYLAGE, ALFALFA" decimal(10,2),
"HAYLAGE, ALFALFA, IRRIGATED" decimal(10,2),
"HAYLAGE, ALFALFA, NON-IRRIGATED" decimal(10,2),
"HAYLAGE, (EXCL ALFALFA)" decimal(10,2),
"HAYLAGE, (EXCL ALFALFA), IRRIGATED" decimal(10,2),
"HAYLAGE, (EXCL ALFALFA), NON-IRRIGATED" decimal(10,2),
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN)" decimal(10,2),
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN), IRRIGATED" decimal(10,2),
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN), NON-IRRIGATED" decimal(10,2),
"LENTILS" decimal(10,2),
"LENTILS, IRRIGATED" decimal(10,2),
"LENTILS, NON-IRRIGATED" decimal(10,2),
"OATS" decimal(10,2),
"OATS, IRRIGATED" decimal(10,2),
"OATS, NON-IRRIGATED" decimal(10,2),
"POTATOES" decimal(10,2),
"SUGARBEETS, IRRIGATED" decimal(10,2),
"SUGARBEETS, NON-IRRIGATED" decimal(10,2),
"WHEAT, WINTER" decimal(10,2),
"WHEAT, WINTER, IRRIGATED" decimal(10,2),
"WHEAT, WINTER, NON-IRRIGATED" decimal(10,2));

create view swap_yield as
select * from crosstab(
'select location,commodity,yield from swap.swap order by 1,2'::text,
'select distinct commodity from swap.swap order by 1'::text
) as (
location varchar(12),
"BARLEY" decimal(10,2),
"BARLEY, IRRIGATED" decimal(10,2),
"BARLEY, NON-IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE" decimal(10,2),
"BEANS, DRY EDIBLE, IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE, LIMA" decimal(10,2),
"BEANS, DRY EDIBLE, LIMA, IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE, LIMA, NON-IRRIGATED" decimal(10,2),
"BEANS, DRY EDIBLE, NON-IRRIGATED" decimal(10,2),
"CORN, GRAIN" decimal(10,2),
"CORN, GRAIN, IRRIGATED" decimal(10,2),
"CORN, GRAIN, NON-IRRIGATED" decimal(10,2),
"CORN, SILAGE" decimal(10,2),
"CORN, SILAGE, IRRIGATED" decimal(10,2),
"CORN, SILAGE, NON-IRRIGATED" decimal(10,2),
"HAY, ALFALFA" decimal(10,2),
"HAY, ALFALFA, IRRIGATED" decimal(10,2),
"HAY, ALFALFA, NON-IRRIGATED" decimal(10,2),
"HAYLAGE, ALFALFA" decimal(10,2),
"HAYLAGE, ALFALFA, IRRIGATED" decimal(10,2),
"HAYLAGE, ALFALFA, NON-IRRIGATED" decimal(10,2),
"HAYLAGE, (EXCL ALFALFA)" decimal(10,2),
"HAYLAGE, (EXCL ALFALFA), IRRIGATED" decimal(10,2),
"HAYLAGE, (EXCL ALFALFA), NON-IRRIGATED" decimal(10,2),
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN)" decimal(10,2),
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN), IRRIGATED" decimal(10,2),
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN), NON-IRRIGATED" decimal(10,2),
"LENTILS" decimal(10,2),
"LENTILS, IRRIGATED" decimal(10,2),
"LENTILS, NON-IRRIGATED" decimal(10,2),
"OATS" decimal(10,2),
"OATS, IRRIGATED" decimal(10,2),
"OATS, NON-IRRIGATED" decimal(10,2),
"POTATOES" decimal(10,2),
"SUGARBEETS, IRRIGATED" decimal(10,2),
"SUGARBEETS, NON-IRRIGATED" decimal(10,2),
"WHEAT, WINTER" decimal(10,2),
"WHEAT, WINTER, IRRIGATED" decimal(10,2),
"WHEAT, WINTER, NON-IRRIGATED" decimal(10,2));

create view swap_yield_unit as
select * from crosstab(
'select 1,commodity,unit from swap.swap order by 1,2,3'::text,
'select distinct commodity from swap.swap order by 1'::text
) as (
location varchar(12),
"BARLEY" text,
"BARLEY, IRRIGATED" text,
"BARLEY, NON-IRRIGATED" text,
"BEANS, DRY EDIBLE" text,
"BEANS, DRY EDIBLE, IRRIGATED" text,
"BEANS, DRY EDIBLE, LIMA" text,
"BEANS, DRY EDIBLE, LIMA, IRRIGATED" text,
"BEANS, DRY EDIBLE, LIMA, NON-IRRIGATED" text,
"BEANS, DRY EDIBLE, NON-IRRIGATED" text,
"CORN, GRAIN" text,
"CORN, GRAIN, IRRIGATED" text,
"CORN, GRAIN, NON-IRRIGATED" text,
"CORN, SILAGE" text,
"CORN, SILAGE, IRRIGATED" text,
"CORN, SILAGE, NON-IRRIGATED" text,
"HAY, ALFALFA" text,
"HAY, ALFALFA, IRRIGATED" text,
"HAY, ALFALFA, NON-IRRIGATED" text,
"HAYLAGE, ALFALFA" text,
"HAYLAGE, ALFALFA, IRRIGATED" text,
"HAYLAGE, ALFALFA, NON-IRRIGATED" text,
"HAYLAGE, (EXCL ALFALFA)" text,
"HAYLAGE, (EXCL ALFALFA), IRRIGATED" text,
"HAYLAGE, (EXCL ALFALFA), NON-IRRIGATED" text,
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN)" text,
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN), IRRIGATED" text,
"HAY, TAME, (EXCL ALFALFA & SMALL GRAIN), NON-IRRIGATED" text,
"LENTILS" text,
"LENTILS, IRRIGATED" text,
"LENTILS, NON-IRRIGATED" text,
"OATS" text,
"OATS, IRRIGATED" text,
"OATS, NON-IRRIGATED" text,
"POTATOES" text,
"SUGARBEETS, IRRIGATED" text,
"SUGARBEETS, NON-IRRIGATED" text,
"WHEAT, WINTER" text,
"WHEAT, WINTER, IRRIGATED" text,
"WHEAT, WINTER, NON-IRRIGATED" text);
