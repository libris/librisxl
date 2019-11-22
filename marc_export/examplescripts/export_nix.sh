#!/bin/bash

# Det här skriptet kan användas som exempel på hur man automatiskt hämtar poster från Libris
# Innan du använder det, se till att du fyllt i filen: etc/export.properties
#
# Lämpligen körs detta skript minut-vis m h a cron.

set -e

# Se till att vi inte kör flera instanser av skriptet samtidigt
[ "${FLOCKER}" != "$0" ] && exec env FLOCKER="$0" flock -en "$0" "$0" "$@" || :

# Om vi kör för första gången, sätt 'nu' till start-tid
LASTRUNTIMEPATH="lastRun.timestamp"
if [ ! -e $LASTRUNTIMEPATH ]
then
    date -u +%Y-%m-%dT%H:%M:%SZ > $LASTRUNTIMEPATH
fi

# Avgör vilket tidsintervall vi ska hämta
STARTTIME=`cat $LASTRUNTIMEPATH`
STOPTIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Hämta data
curl --fail -XPOST "https://libris.kb.se/api/marc_export/?from=$STARTTIME&until=$STOPTIME&deleted=ignore&virtualDelete=false" --data-binary @./etc/export.properties > export.txt

# Om allt gick bra, uppdatera tidsstämpeln
echo $STOPTIME > $LASTRUNTIMEPATH

# DINA ÄNDRINGAR HÄR, gör något produktivt med datat i 'export.txt', t ex:
# cat export.txt
