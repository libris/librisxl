# Libris-XL REST

## APIs and URLs
whelk, DOCUMENT API:
http://\<host\>:\<PORT\>/bib/7149593

ELASTIC SEARCH, INDEX SEARCH API:
Find 'tove' in libris index, index-typedoc 'person':
http://\<host\>:9200/libris/auth/\_search?q=tove

ELASTIC SEARCH, ANALYZE INDEXED VALUES FOR A SPECIFIC FIELD:
curl -XGET http://\<host\>:9200/libris/auth/\_search -d '{ "facets" : { "my_terms" : { "terms" : { "size" : 50, "field" : "about.controlledLabel.raw" } } } }'

whelk, SEARCH API:
http://\<host\>:\<PORT\>/\<indextype\>/?\<field\>=\<value\>

INDEX TYPES:
bib, auth, person, def, sys

EXPAND AUTOCOMPLETE:
http://\<host\>:\<PORT\>/\_expand?name=Jansson,%20Tove,%201914-2001.

REMOTESEARCH
http://\<host\>:\<PORT\>/\_remotesearch?q=astrid

HOLDINGCOUNT
http://\<host\>:\<PORT\>/\_libcount?id=/resource/bib/7149593
