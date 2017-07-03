#!/bin/bash -x

DATE=`date +"%Y-%m-%d"`
#DATE='2017-05-10'
HOME=/appl/import2

$HOME/scripts/oaiharvest.py "http://www.diva-portal.org/dice/oai?verb=ListRecords&metadataPrefix=marc21&set=libris&from=$DATE" $HOME/queues/diva/incoming/$DATE

# OBS electronic via eplikt
#$HOME/scripts/oaiharvest.py "http://www.diva-portal.org/dice/oai?verb=ListRecords&metadataPrefix=marc21electronic&set=libris-electronic&from=$DATE" $HOME/queues/diva/incoming/"$DATE"_electronic
