#! /usr/bin/make -f

#ifndef configure.mk
#include ../configure.mk
#endif
SHELL:=/bin/bash
PG:=psql -d nass

# If included somewhere else
nass.mk:=1


INFO::
	@echo NASS - Quickstats
	@echo ${quickstats.csv}

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

outs:=$(patsubst %,../%.csv,county_adc land_rent \
	commodity_harvest commodity_yield commodity_price \
	commodity_list  )

db/nass.sql: ${quickstats}
	${PG} -c 'truncate quickstats.quickstats; insert into quickstats.quickstats select distinct * from quickstats.quickstats_raw'
	${PG} -f nass.sql -d nass;
#	${PG} -f nass_cmz.sql -d nass;
	touch $@

.PHONY:outs
outs:${outs}

$(outs):../%.csv:db/nass.sql
	${PG} -c '\COPY (select * from nass.$*) to $@ with csv header';

