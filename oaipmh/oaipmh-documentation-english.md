# Documentation for Libris XL OAI-PMH implementation.

The Libris OAI-PMH server implementation conforms to the official OAI-PMH specifikationen which can be accessed [here](https://www.openarchives.org/OAI/openarchivesprotocol.html).

The purpose of this document is to provide Libris-specific information on to how to use OAI-PMH to harvest metadata from Libris.

## The Set-parameter:

The OAI-PMH specification describes harvesting of sets. Libris uses sets for certain specific purposes.

Libris separates metadata into three primary sets, which are: `auth`, `bib`, och `hold`. These represent authority records, bibliographic records och holding records respectively.

In addition to the primary sets, there are subsets for bibliographic and holding records relating to specific "sigel" (library codes).
For example `set=bib:S` which contains all bibliographic records for which there exists a holding record with the sigel S.
The subset `set=hold:S` contains all holding record with the sigel S.

## The MetadataPrefix-parameter:

OAI-PMH uses the "metadataPrefix" parameter to select a format in which to deliver data. The Libris OAI-PMH implementation supports three primary formats in addition to the required `oai_dc` (Dublin Core) format. These are `marcxml`, `rdfxml` and `jsonld`.

To cover the internal needs of Libris, each of the primary formats is also available in three different derivative formats. Strictly speaking these are formats in their own right, but in practice they are used as a mechanism for passing extra configuration information to the server. The derivative formats are:
* `[primaryFormat]_expanded`
* `[primaryFormat]_includehold`
* `[primaryFormat]_includehold_expanded`

`[primaryFormat]_expanded` for a record `A` means that information from records `B,C ..` to which `A` have links is also (where possible) added to `A`.
For example if `metadataPrefix=marcxml_expanded` is used for a bilbiographic record, the result is that relevant authority information is added to the bibliographic record.

`[primaryFormat]_includehold` only differs from  `[primaryFormat]` för bibliographic records. Each record is then delivered with a list of holding records attached to the bibliographic record in question (the list of holding records sits in the <about> part of the response, for each record).

`[primaryFormat]_includehold_expanded` combines `[huvudformat]_includehold` and `[huvudformat]_expanded`

## Libris specific parameters:
The Libris OAI-PMH implementation allows one extra parameter which is not a part of the OAI-PMH specification. This parameter is called `x-withDeletedData` and may be used with the verbs `GetRecord` and `ListRecords`. If `x-withDeletedData` is set to `true` this results in data being delivered for records even if those records are marked deleted. This violates the OAI-PMH specification, which explicitly forbids both extra parameters and delivering deleted data. The parameter has been included anyway, because it is necessary for certain Libris functionality.

#### Example
To harvest (from the Libris QA-environment) all bibliographic records (with a list of holding records attached) for which there exists holding records with the sigel `S` and that have been modified between 13/2 and 12:00 14/2 (UTC):

```
$ curl "https://libris-qa.kb.se/api/oaipmh/?verb=ListRecords&metadataPrefix=marcxml_includehold_expanded&from=2018-02-13&until=2018-02-14T12:00:00Z&x-withDeletedData=true&set=bib:S"
<?xml version="1.0" encoding="UTF-8"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"><responseDate>2018-02-15T10:54:18.606Z</responseDate><request verb="ListRecords" metadataPrefix="marcxml_includehold_expanded" from="2018-02-13" until="2018-02-14" x-withDeletedData="true" set="hold:S">http://export-qa.libris.kb.se:8080/oaipmh/</request><ListRecords><record><header><identifier>https://libris-qa.kb.se/pt0pfgtv1hf8p2t</identifier>
...
```
