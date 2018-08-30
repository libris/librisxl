#!/usr/bin/env bash

echo `curl -s -o /dev/null -w "%{http_code}" -Hcontent-type:application/xml -XPOST "http://127.0.0.1:8100/hold/12345" -d@mystuff/t1.jsonld`
echo `curl -s -o /dev/null -w "%{http_code}" -Hcontent-type:application/xml -XPOST "http://127.0.0.1:8100/hold/12346" -d@mystuff/t2.jsonld`
echo `curl -s -o /dev/null -w "%{http_code}" -Hcontent-type:application/xml -XPOST "http://127.0.0.1:8100/hold/12347" -d@mystuff/t3.jsonld`
