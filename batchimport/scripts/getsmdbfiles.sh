#!/bin/bash -x

DATE=`date +"%Y-%m-%d"`
#DATE=2011-12-15
HOME=/appl/import2

#$HOME/scripts/oaiharvest.py "http://api.smdb.kb.se/oai?verb=ListRecords&set=audiobooks&metadataPrefix=smdb_libris" $HOME/queues/smdb/incoming/$DATE
$HOME/scripts/oaiharvest.py "http://api.smdb.kb.se/oai-pmh?verb=ListRecords&set=audiobooks&metadataPrefix=smdb_libris&from=$DATE" $HOME/queues/smdb/incoming/$DATE

