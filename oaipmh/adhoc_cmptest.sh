#!/bin/bash

start=$(date --utc -d "today" +%Y-%m-%dT07:%M:00Z)

echo "?verb=ListRecords&set=hold&from=$start&metadataPrefix=jsonld"
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=hold&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=hold&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

echo "?verb=ListRecords&set=bib&from=$start&metadataPrefix=jsonld"
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=bib&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=bib&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

echo "?verb=ListRecords&set=auth&from=$start&metadataPrefix=jsonld"
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=auth&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=auth&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

echo "?verb=ListRecords&set=bib:S&from=$start&metadataPrefix=jsonld"
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=bib:S&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=bib:S&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

echo "?verb=ListRecords&set=hold:S&from=$start&metadataPrefix=jsonld"
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=hold:S&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=hold:S&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

# DIFFING A LITTLE, BECAUSE PROD ATM ONLY FOLLOWS LINKS IN ONE STEP
echo "?verb=ListRecords&set=bib:Li&from=$start&metadataPrefix=marcxml_includehold_expanded"
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=bib:Li&from=$start&metadataPrefix=marcxml_includehold_expanded" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=bib:Li&from=$start&metadataPrefix=marcxml_includehold_expanded" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is expected:"
diff oldcode newcode

# DIFFING A LITTLE, BECAUSE PROD ATM ONLY FOLLOWS LINKS IN ONE STEP
echo "?verb=ListRecords&set=hold&from=$start&metadataPrefix=jsonld_expanded"
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=hold&from=$start&metadataPrefix=jsonld_expanded" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=hold&from=$start&metadataPrefix=jsonld_expanded" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is expected:"
diff oldcode newcode

echo "?verb=ListRecords&set=bib&from=$start&metadataPrefix=jsonld"
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=bib&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=bib&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

echo "?verb=GetRecord&identifier=https://libris.kb.se/6h4rcxkn4fqlm9h5&metadataPrefix=marcxml"
curl -s "localhost:8080/oaipmh/?verb=GetRecord&identifier=https://libris.kb.se/6h4rcxkn4fqlm9h5&metadataPrefix=marcxml" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=GetRecord&identifier=https://libris.kb.se/6h4rcxkn4fqlm9h5&metadataPrefix=marcxml" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

echo "?verb=ListRecords&set=bib&from=$start&until=2020-01-11T11:00:00Z&metadataPrefix=jsonld"
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=bib&from=$start&until=2020-01-11T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=bib&from=$start&until=2020-01-11T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

echo "?verb=ListRecords&from=$start&metadataPrefix=jsonld"
curl -s "localhost:8080/oaipmh/?verb=ListRecords&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&from=$start&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode
