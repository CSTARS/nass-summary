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

db/nass:
	[[ -d db ]] || mkdir db
	${PG} -f nass.sql
	touch $@

quickstats:=$(patsubst %,db/%,$(shell echo quickstats/*.csv))

.PHONY:quickstats
quickstats:${quickstats}

${quickstats}:db/%:db/nass
	${PG} -c '\COPY nass.quickstats FROM $* CSV HEADER'
	touch $@

outs:=$(patsubst %,%.csv,county_adc land_rent)

.PHONY:outs
outs:${outs}

$(outs):%.csv:
	${PG} -c '\COPY (select * from nass.$*) to $*.csv with csv header';
