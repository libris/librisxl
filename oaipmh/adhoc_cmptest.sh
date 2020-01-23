#!/bin/bash

curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=hold&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=hold&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=bib&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=bib&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

#OK BUT TAKES TIME
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=auth&from=2020-01-20T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=auth&from=2020-01-20T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=bib:S&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=bib:S&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=hold:S&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=hold:S&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

# DIFFING A LITTLE, BECAUSE PROD ATM ONLY FOLLOWS LINKS IN ONE STEP
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=bib:Li&from=2020-01-23T11:00:00Z&metadataPrefix=marcxml_includehold_expanded" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=bib:Li&from=2020-01-23T11:00:00Z&metadataPrefix=marcxml_includehold_expanded" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is expected:"
diff oldcode newcode

# DIFFING A LITTLE, BECAUSE PROD ATM ONLY FOLLOWS LINKS IN ONE STEP
curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=hold&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld_expanded" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=hold&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld_expanded" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is expected:"
diff oldcode newcode

curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=bib&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=bib&from=2020-01-23T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

curl -s "localhost:8080/oaipmh/?verb=GetRecord&identifier=https://libris.kb.se/6h4rcxkn4fqlm9h5&metadataPrefix=marcxml" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=GetRecord&identifier=https://libris.kb.se/6h4rcxkn4fqlm9h5&metadataPrefix=marcxml" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode

curl -s "localhost:8080/oaipmh/?verb=ListRecords&set=bib&from=2020-01-10T11:00:00Z&until=2020-01-11T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > newcode &
curl -s "export-prod.libris.kb.se:8080/oaipmh/?verb=ListRecords&set=bib&from=2020-01-10T11:00:00Z&until=2020-01-11T11:00:00Z&metadataPrefix=jsonld" | xmllint --format - | wc -l > oldcode &
wait
echo "Diff is:"
diff oldcode newcode
