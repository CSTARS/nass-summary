#! /usr/bin/make -f

#ifndef configure.mk
#include ../configure.mk
#endif
SHELL:=/bin/bash
PG:=psql -d nass

# If included somewhere else
nass.mk:=1

# This key is not included, you need to get one yourself, the
include usda.key

INFO::
	@echo NASS - Quickstats
	@echo ${quickstats.csv}
	@echo USDA KEY : ${usda.key}

# California Ag Commissioner's data
include ca-agcomm.mk

db/quickstats.sql:
	[[ -d db ]] || mkdir db
	${PG} -f quickstats.sql
	touch $@

quickstats:=$(patsubst %,db/%,$(shell echo quickstats/*.csv))

.PHONY:quickstats
quickstats:${quickstats}

${quickstats}:db/%:db/quickstats.sql
	${PG} -c '\COPY quickstats.quickstats_raw FROM $* CSV HEADER'
	touch $@

db/nass.sql: ${quickstats}
	${PG} -c 'truncate quickstats.quickstats; insert into quickstats.quickstats select distinct * from quickstats.quickstats_raw'
	${PG} -f nass.sql -d nass;
#	${PG} -f nass_cmz.sql -d nass;
	touch $@

outs:=$(patsubst %,../%.csv,county_adc land_rent \
	commodity_harvest commodity_county_yield commodity_yield commodity_price \
	commodity_list)


swapouts:=$(patsubst %,../%.csv,harvest_2012 swap swap_net swap_yield \
swap_price swap_yield_unit)

.PHONY:outs
outs:${outs}

$(outs):../%.csv:db/nass.sql
	${PG} -c '\COPY (select * from nass.$*) to $@ with csv header';

swap:${swapouts}

$(swapouts):../%.csv:
	${PG} -c '\COPY (select * from swap.$*) to $@ with csv header';


# Yu Pei found a nice API for the NASS data, unfortunately, one
# problem with it is the format of the data is now different.  It's
# better however, so we should try and get it this way, but I am
# slowly moving from quickstats.

states:=CA WA ID MT OR

years:=2007 2008 2009 2010 2011 2012 2013 2014

stats:=PRICE+RECEIVED YIELD PRODUCTION AREA+HARVESTED WATER+APPLIED \
AREA+BEARING AREA+IRRIGATED AREA+IRRIGATED+PREVIOUS+CENSUS+YEAR AREA+CROPLAND


usda.get=http://quickstats.nass.usda.gov/api/api_GET?key=${usda.key}&format=JSON&freq_desc=ANNUAL

usda.parm=http://quickstats.nass.usda.gov/api/get_param_values?key=${usda.key}&format=JSON

empty:=
space:=${empty} ${empty}
comma:=,

usda.states:=$(subst ${space},&,$(patsubst %,state_alpha=%,${states}))
usda.com:=$(subst ${space},&,$(patsubst %,commodity_desc=%,${commodities}))
usda.stats:=$(subst ${space},&,$(patsubst %,statisitccat_desc=%,${stats.harvest}))


columns:=year commodity_desc statisticcat_desc county_code source_desc \
	unit_desc prodn_practice_desc freq_desc \
	domain_desc util_practice_desc value reference_period_desc \
	class_desc asd_code agg_level_desc domaincat_desc \
	state_fips_code state_alpha group_desc

jq.col:=$(subst ${space},${comma},$(patsubst %,.%,${columns}))

.PHONY: usda_api.json usda_api.db

define s_p_s_y 
usda_api.json::usda_api/$1-$2-$3-$4.json
usda_api.db::usda_api/$1-$2-$3-$4.db

usda_api/$1-$2-$3-$4.json:
	curl "${usda.get}&state_alpha=$1&source_desc=$2&statisticcat_desc=$3&year=$4" > $$@

usda_api/$1-$2-$3-$4.db:usda_api/$1-$2-$3-$4.json
	${PG} -c "delete from quickstats.usda_api where state_alpha='$1' and source_desc='$2' and statisticcat_desc='$3' and year='$4';";\
	jq --raw-output '.data | .[] | [${jq.col}] | @csv' < $$< |\
	${PG} -c 'copy quickstats.usda_api from stdin with csv';\
	${PG} -c "select count(*) from quickstats.usda_api where state_alpha='$1' and source_desc='$2' and statisticcat_desc='$3' and year='$4';" >$$@

endef

$(foreach s,${states},$(foreach p,SURVEY CENSUS,$(foreach x,${stats},$(foreach y,${years},$(eval $(call s_p_s_y,$s,$p,$x,$y))))))

define s-RENT-y
usda_api.json::usda_api/$1-RENT-$2.json
#usda_api.db::usda_api/$1-RENT-$2.db

usda_api/$1-RENT-$2.json:
	curl "${usda.get}&state_alpha=$1&commodity_desc=RENT&year=$2" > $$@

usda_api/$1-RENT-$2.csv:usda_api/$1-RENT-$2.json

usda_api/$1-RENT-$2.db:usda_api/$1-RENT-$2.json
	${PG} -c "delete from quickstats.usda_api where state_alpha='$1' and commodity_desc='RENT' and year='$2';"; \
	jq --raw-output '.data | .[] | [${jq.col}] | @csv' < $$< |\
	${PG} -c '\COPY quickstats.usda_api from stdin with csv';\
	${PG} -c "select count(*) from quickstats.usda_api where state_alpha='$1' and commodity_desc='RENT' and year='$2';" >$$@

endef

$(foreach s,${states},$(foreach y,${years},$(eval $(call s-RENT-y,$s,$y))))


usda_api/region.csv:usda_api.json
	for j in usda_api/*.json; do \
	  jq --raw-output '.data | .[] | [.state_alpha,.state_fips_code,.county_code,.county_name,.asd_code,.asd_desc] | @csv' < $$j; \
	done | sort -u > $@

commodity_desc.json statisticcat_desc.json:%.json:
	curl '${usda.parm}&param=$*'  > $@

##jq '.' < statisticcat_desc.json
