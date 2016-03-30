#! /usr/bin/make -f 

ca-agcomm.years:=2007 2008 2009 2010 2011 2012 2013
ca-agcomm.url:=http://www.nass.usda.gov/Statistics_by_State/California/Publications/Ca-AgcommComm

ca-agcomm.2013:=2013cropyear
ca-agcomm.2012:=201212cactb00
ca-agcomm.2011:=201112cactb00
ca-agcomm.2010:=201010cactb00
ca-agcomm.2009:=200910cactb00
ca-agcomm.2008:=200810cactb00
ca-agcomm.2007:=200708cactb00

csv:=$(patsubst %,ca-agcomm/%.csv,${ca-agcomm.years})

.PHONY: ca.csv
ca-agcomm.csv:${csv}

${csv}:ca-agcomm/%.csv:
	curl ${ca-agcomm.url}/${ca-agcomm.$*}.csv > $@

ca-agcomm.db:
	${PG} -c 'truncate quickstats.agcomm';
	cat ${csv} | grep -v '^Year' | sed -e 's/ *, */,/g;' -e 's/\x0D$$//' | ${PG} -c "copy quickstats.agcomm from stdin with csv"
