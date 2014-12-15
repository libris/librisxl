#!/bin/bash

SINCE=`cat /var/run/oaipmh/last.ts`

if [[ "$SINCE" == "" ]]; then
    SINCE=`date +%Y-%m-%dT%H:%M:%SZ -d yesterday`
fi

RESPONSE=`curl -s -o /dev/null -w "%{http_code}" -XPOST "http://hp01:8080/whelk/_operations" -d "operation=import&importer=oaipmhimporter&dataset=auth,bin,hold&since=$SINCE"`
if [[ "$RESPONSE" == "303" ]]; then
    echo -n `date +%Y-%m-%dT%H:%M:%SZ` > /var/run/oaipmh/last.ts
fi

