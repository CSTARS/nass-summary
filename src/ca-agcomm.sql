set search_path=quickstats,public;

create function agcomm_init() returns boolean as $$
BEGIN
IF EXISTS (SELECT relname FROM pg_class WHERE relname='agcom') 
THEN
truncate quickstats.agcomm;
ELSE
create table agcomm(
year integer, 
commodiy_code integer, 
crop text,
county_code varchar(12), 
county text,
harvested_acres float, 
yield float, 
production float, 
price float,
unit text, 
value float
);
END
$$ LANGUAGE PGPLSQL;


