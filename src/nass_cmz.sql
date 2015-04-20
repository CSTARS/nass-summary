create table nass.cmz_cnty as
select lower(regexp_replace(cmz,' ','','g')) as cmz,
fips,
st_intersection(z.geom,c.boundary) as boundary
from cmz.cmz_pnw z
join national_atlas.county c
on st_intersects(z.geom,c.boundary);

create view cmz_fips_fraction as 
with f as (
 select cmz,fips,
 (st_area(z.boundary)/st_area(c.boundary))::decimal(6,2) as fraction
 from nass.cmz_cnty z
 join national_atlas.county c using (fips)
 )
 select * from f where fraction>0 order by 2,1;

create or replace view cmz_commodity_total_harvest as
select
commodity,cmz as location,year,
sum(irrigated*fraction)::integer as irrigated,
sum(non_irr*fraction)::integer as non_irr,
sum(total*fraction)::integer as total
from commodity_total_harvest
join cmz_fips_fraction on (location=fips)
group by commodity,cmz,year
having sum(total*fraction)::integer>0
order by 3,2,1;

--\COPY (select * from cmz_commodity_total_harvest) to cmz_commodity_total_harvest.csv with csv header
