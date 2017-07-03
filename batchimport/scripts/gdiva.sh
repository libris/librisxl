#!/bin/bash -x

#DATE=`date +"%Y-%m-%d"`
DATE='2016-02-26'
HOME=/appl/import2

$HOME/scripts/oaiharvest.py "http://www.diva-portal.org/dice/oai?verb=ListRecords&metadataPrefix=marc21&set=libris&from=$DATE&until=$DATE" /tmp/diva.2015
#$HOME/scripts/oaiharvest.py "http://www.diva-portal.org/dice/oai?verb=ListRecords&metadataPrefix=marc21&set=libris&from=$DATE&until=$DATE" $HOME/queues/diva/incoming/$DATE
#$HOME/scripts/oaiharvest.py "http://www.diva-portal.org/dice/oai?verb=ListRecords&metadataPrefix=marc21electronic&set=libris-electronic&from=$DATE&until=$DATE" $HOME/queues/diva/incoming/"$DATE"_electronic

