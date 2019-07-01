# MARCFRAME

> The purpose of this document is for developers and others who need to understand the mechanisms of MARCFRAME, the configuration file which handles conversion of MARC21 to JSON-LD and the roundtrip JSON-LD to MARC21. The context is the Libris implementation of MARC21.

## Structure

Marcframe is comprised of mainly three sections which represents three different MARC21 formats, bibliographic, authority and holdings. In addition there is an introductory part which contains reusable patterns mappings of codes to terms.


```
{
  {bib},
  {auth},
  {hold}
}
```

For each section there is a chronological mapping of the fields 000-999 and their subfields to properties and classes which are definied either in the kbv: vocabulary or the group of terms which get a marc: prefix. Marc terms are used where the conversion do not fit in a Bibframe 2 form or the usage has in other ways been ambiguous.

The most common reason for a marc-prefix is LC combining two fields so it is not possible to separate the data making a reconversion impossible, or that they haven't made any attempts to code the information either with nac - no attempt to code, or ignore for things which have no meaning outside MARC21.

There are also certain postprocessing steps which can handle particular fields, for example date/year which needs to be exported to several fields (E.g. 008 + 264) depending on how they are formatted.

## Syntax in MARCFRAME

This is a list of keys in Marcframe which control certain functionality like repetition, grouping, inclusion of patterns etc. 

`about` enables shortforms of more complicated or nested entitites. Used with pendingResource.

`aboutAlias` used to include an about form.

`aboutEntity` Defines which entity the property is a part of, **thing** means instance in bibliographic data.

`aboutType` -

`addLink` Is used when the entities (classes) are repeatable. If the entity can only occur once use `link`. The repetition of MARC21 fields is stated either in [Libris Formathandbok](https://www.kb.se/katalogisering/Formathandboken) or [LC MARC21 specification](https://www.loc.gov/marc/).

`addProperty`(R) and `property` (NR) is used to define whether a property is repeatable or not.

`embedded` could be set to *true* which means that the entity is a so called structured value and it's value is always embedded locally to the graph and not linked to. In RDF also known as a b-node. The most common types is titles, identifiers and notes.

`fixedDefault` a fixed default value in indicators and fixed fields if none exists in the data. Only for conversion from RDF to MARC21.  

`include` includes the pattern which match the value.

`inherit` creates an inheritance from the specified field.

`leadingPunctuation` see punctuationChars.

`link` see addLink.

`match` used for matching values in indicator or occurence of fields with a `when` construction. [See exemple 2, which illustrates matching the right type of agent or work in the **$100** field]

`pendingResource` makes it possible to create an hierarchical structure of entities.

`property` see addProperty.

`punctuationChars` states the interpunctuation which is stripped away and restored to the string/subfield in the conversion. Variants in usage is leadingPunctuation, trailingPunctuation and surroundingChars.

`required: true` is used in the conversion to MARC21. A required subfield is mandatory for the field to convert. If several subfields could be considered useful to surmise meaning but they are not all required, you could instead use the `supplementary: true` for the fields which can not standalone. [Example: `$a` is often required, `$2` is often supplementary].

`resourceType` the entity which is created through the link or addlink.

`spec` used to verify indata and outdata as MARC21 conforms to expected forms. Can be specified with `source` (indata) and `normalized` (outdata), if normalize is absent it is by default expected to look like the incoming MARC21 source. In the spec we also define how our expected JSON-LD form looks like. If you make several testcases you can name them to let others know if there is a particular scenario or deviation you are testing.

`splitValuePattern` the regexp pattern used to separate strings into properties. [Example: identifier with trailing qualifier].

`splitValueProperties` defines the properties which the separated strings groups should map to. [Example: value, qualifier].

`subfieldOrder` used to specify subfield order in conversion to MARC21. If subfield order is absent alphabetical order is applied. [Example: `“a q c z”`].

`surroundingChars` see punctuationChars.

`tokenmap` used for mapping a list of (mostly) single character values from indicators or fixed fields.

`trailingPunctuation` see punctuationChars.

`uriTemplate` the base used for `matchUriToken` to create a link, usually from a code.

`when` see match.

### Example 1. BIB 020
```
    "020": {
      "include": ["identifier"],
      "resourceType": "ISBN",
      "$c": {"property": "acquisitionTerms"},
      "$6": {"property": "marc:fieldref"},
      "$8": {"property": "marc:groupid"},
      "subfieldOrder": "a q c z"
     }
```
For **020** there is an `include` which means it picks the pattern from the value `“identifier”`.
```
    "identifier": {
      "include": ["idbase"],
      "$a": {
        "property": "value",
        "splitValuePattern": "^(.+?)(?:\\s+\\((.+)\\))?$",
        "splitValueProperties": ["value", "qualifier"],
        "punctuationChars": ",:;"
      },
      "$q": {"addProperty": "qualifier"},
      "$z": {"addProperty": "marc:hiddenValue"}
    },
```
For identifier you can also see it includes an idbase.
```
    "idbase": {
      "aboutEntity": "?thing",
      "addLink": "identifiedBy",
      "embedded": true
    },
```
So in total a 020 mapping could look like this.
```
    "020": {
      "aboutEntity": "?thing",
      "addLink": "identifiedBy",
      "embedded": true
      "resourceType": "ISBN",
      "$a": {
        "property": "value",
        "splitValuePattern": "^(.+?)(?:\\s+\\((.+)\\))?$",
        "splitValueProperties": ["value", "qualifier"],
        "punctuationChars": ",:;"
      },
      "$q": {"addProperty": "qualifier"},
      "$z": {"addProperty": "marc:hiddenValue"}  
      "$c": {"property": "acquisitionTerms"},
      "$6": {"property": "marc:fieldref"},
      "$8": {"property": "marc:groupid"},
      "subfieldOrder": "a q c z"
}
```

### Example 2. BIB 100

Match and When shows conditions for mapping depending on indicator value or presence of subfields.
```
    "100" : {
      "match": [
        {
          "when": "$t & i1=0",
          "aboutAlias": "_:work",
          "include": ["contributionPerson", "titledetails"]
        },
        {
          "when": "$t & i1=1",
          "aboutAlias": "_:work",
          "include": ["contributionNameOfPerson", "titledetails"]
        },
        {
          "when": "$t & i1=3",
          "aboutAlias": "_:work",
          "include": ["contributionFamily", "titledetails"]
        },
        {
          "when": "$t",
          "aboutAlias": "_:work",
          "include": ["contributionPerson", "titledetails"]
        },
        {
          "when": "i1=0",
          "aboutEntity": "?thing",
          "aboutType": "Person",
          "aboutAlias": "_:agent",
          "include": "agentdataPerson"
        },
        {
          "when": "i1=1",
          "aboutEntity": "?thing",
          "aboutType": "Person",
          "aboutAlias": "_:agent",
          "include": "agentdataNameOfPerson"
        },
        {
          "when": "i1=3",
          "aboutEntity": "?thing",
          "aboutType": "Family",
          "aboutAlias": "_:agent",
          "include": "agentdataFamily"
        },
        {
          "when": null,
          "aboutEntity": "?thing",
          "aboutType": "Person",
          "aboutAlias": "_:agent",
          "include": "agentdataPerson"
        }
      ],
      "$6": {"property": "marc:fieldref"},
      "subfieldOrder": "a b c q d j g e 4 t n p l k f o v x y z"
    }
```


## Sources for mapping


- [BIBFRAME 2 conversion specifications](https://www.loc.gov/bibframe/)
- [Libris Formathandbok](https://www.kb.se/katalogisering/Formathandboken)
- [LC MARC21 specification](https://www.loc.gov/marc/)
- [OCLC](https://www.oclc.org/bibformats/en.html)
- Translations and configuration files from old Libris systems.
- Statistics about field occurence.

In Marcframe there also occurs notes for these.

| Note | Description |
| --- | --- |
| NOTE: | General note. |
| NOTE:local | About local use from Libris formathandbok. |
| NOTE:LC | Note concerning Library of Congress MARC specification or BF2 mappning. |
| NOTE:OCLC | OCLC defined fields which commonly occurs in imported records. |



