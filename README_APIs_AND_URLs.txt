WHELK-WEBAPI, DOCUMENT API:
http://<host>:8080/whelk-webapi/bib/7149593

ELASTIC SEARCH, INDEX SEARCH API:
Find 'tove' in libris index, index-typedoc 'person':
http://<host>:9200/libris/person/_search?q=tove

ELASTIC SEARCH, INDEX SEARCH API:
Find 'tove' in libris index, index-typedoc 'concept':
http://<host>:9200/libris/concept/_search?q=tove

ELASTIC SEARCH:
PUT mapping with prop config:
curl -XPUT http://<host>:9200/libris/bib/_mapping -d@etc/elastic_mappings.json

AUTOCOMPLETE, PERSON:
http://<host>:8080/whelk-webapi/_complete?name=tove

AUTOCOMPLETE, CONCEPT:
http://<host>:8080/whelk-webapi/_subject?name=tove

EXPAND AUTOCOMPLETE:
http://<host>:8080/whelk-webapi/_expand?name=Jansson,%20Tove,%201914-2001.

