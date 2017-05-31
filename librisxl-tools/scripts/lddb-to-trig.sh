(
echo '['
psql whelk -tc "select data from lddb;" | sed 's/^/{"@graph": /' | sed 's/$/ } ,/' | sed 's/^{"@graph":  } ,$/{}/g'
echo ']'
) | rdfpipe -ijson-ld:context=../definitions/build/vocab/context.jsonld -otrig -
