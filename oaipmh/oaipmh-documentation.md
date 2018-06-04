# Dokumentation för Libris XL OAI-PMH implementation.

Libris OAI-PMH server följer den officiella OAI-PMH specifikationen som kan läsas [här](https://www.openarchives.org/OAI/openarchivesprotocol.html).

Det här dokumentets syfte är att ge Libris-specifik information, om hur OAI-PMH kan användas för att hämta Libris-metadata.

## Set-parametern (delmängder):

OAI-PMH specifikationen beskriver hur skördning av delmängder fungerar generellet, men i Libris används dessa för vissa specifika ändamål.

Libris OAI-PMH implementation använder sig av tre huvudsakliga delmängder för sitt metadata. Dessa är `auth`, `bib`, och `hold`, vilka representerar auktoritetsposter, bibliografiska poster och beståndsposter.

Utöver de tre grundläggande delmängderna finns det under-delmängder för bibliografiska och beståndsposter för specifika sigel.
T ex utgör delmängden `set=bib:S` alla bibliografiska poster för vilka det finns en beståndspost med sigel S.
Delmängden `set=hold:S` utgörs av alla beståndsposter med sigel S.

## MetadataPrefix-parametern (format):

OAI-PMH använder sig av parametern "metadataPrefix" för att välja export-format. Libris OAI-PMH implementation stödjer huvudsakligen tre format, vilka är `marcxml`, `rdfxml` och `jsonld`. Eftersom OAI-PMH specifikationen kräver detta så erbjuds också en extremt enkel form av `oai_dc` (Dublin Core), men i praktiken saknar poster i `oai_dc` all egentlig metadata.

För att täcka Libris behov utan att bryta mot OAI-PMH specifikationen så erbjuds ytterligare tre "former" av dessa format (strikt taget engligt specifikationen är dessa fristående format i sig).
Dessa är:
* `[huvudformat]_expanded`
* `[huvudformat]_includehold`
* `[huvudformat]_includehold_expanded`

`[huvudformat]_expanded` för en post `A` innebär att poster `B,C ..` som `A` länkar till också i möjligaste mån också monteras in i `A`.
T ex: Om `metadataPrefix=marcxml_expanded` används för en bibliografisk post så innebär detta att relevant auktoritetsinformation bakas in i posten.

`[huvudformat]_includehold` skiljer sig bara ifrån `[huvudformat]` för bibliografiska poster. Dessa poster följs då av en lista med beståndsposter för den bibliografiska posten i fråga (listan finns i `<about>`-delen av svaret för posten).

`[huvudformat]_includehold_expanded` kombinerar `[huvudformat]_includehold` och `[huvudformat]_expanded`

## Librisspecifika parametrar:
Libris implementation av OAI-PMH erbjuder en extra parameter som inte ingår i OAI-PMH specifikationen och som kan användas tillsammans med verben `GetRecord` och `ListRecords`. Parametern heter `x-withDeletedData` och om den sätts till `true` så inkluderar svaret metadata även för poster som markerats som borttagna. Detta strider mot OAI-PMH specifikationen som förbjuder både extra parametrar och att metadata för borttagna poster skickas med. Trots detta har valet gjorts att använda den här parametern för att möjliggöra viss nödvändig libris-funktionalitet.

#### Exempel
För att (från Libris QA mijlö) hämta alla bibliografiska poster (med tillhörande beståndsposter), för vilka det finns beståndsposter med sigel `S` och som uppdaterats mellan den 13 och klockan 12 den 14 februari:

```
$ curl "export-qa.libris.kb.se:8080/oaipmh/?verb=ListRecords&metadataPrefix=marcxml_includehold_expanded&from=2018-02-13&until=2018-02-14T12:00:00Z&x-withDeletedData=true&set=bib:S"
<?xml version="1.0" encoding="UTF-8"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"><responseDate>2018-02-15T10:54:18.606Z</responseDate><request verb="ListRecords" metadataPrefix="marcxml_includehold_expanded" from="2018-02-13" until="2018-02-14" x-withDeletedData="true" set="hold:S">http://export-qa.libris.kb.se:8080/oaipmh/</request><ListRecords><record><header><identifier>https://libris-qa.kb.se/pt0pfgtv1hf8p2t</identifier>
...
```
