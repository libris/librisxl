WHELK-WEBAPI, DOCUMENT API:
http://<host>:8080/whelk-webapi/bib/7149593

ELASTIC SEARCH, INDEX SEARCH API:
Find 'tove' in libris index, index-typedoc 'person':
http://<host>:9200/libris/person/_search?q=tove

ELASTIC SEARCH, MAPPING:
PUT mapping with prop config:
curl -XPUT http://<host>:9200/libris/bib/_mapping -d@whelk-webapi/src/main/resources/elastic_mappings.json

ELASTIC SEARCH, ANALYZE INDEXED VALUES FOR A SPECIFIC FIELD:
curl -XGET http://<host>:9200/libris/auth/_search -d '{ "facets" : { "my_terms" : { "terms" : { "size" : 50, "field" : "about.controlledLabel.untouched" } } } }'

WHELK-WEBAPI, SEARCH API:
http://<host>:8080/whelk-webapi/<indextype>/q=(<field>:)strindberg

INDEX TYPES:
bib, auth, person, concept, organization, conceptscheme ...

EXPAND AUTOCOMPLETE:
http://<host>:8080/whelk-webapi/_expand?name=Jansson,%20Tove,%201914-2001.


