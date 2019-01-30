# MARCFRAME

> The purpose of this document is for developers and others who need to understand the mechanisms of MARCFRAME, the configuration file which handles conversion of MARC21 to JSON-LD and the roundtrip JSON-LD to MARC21. The context is the Libris implementation of MARC21, hence the documentation is in Swedish. 

## Struktur
Marcframe är uppbyggd med tre huvudsakliga sektioner. Därutöver även en introducerande del som behandlar återkommande mappningsmönster (patterns) och koppling av koder till termer.

De tre huvudsektionerna kommer ur tre olika MARC21 format. Bibliografiska formatet, Auktoritetsformatet och Beståndsformatet. 

```
{
  {bib},
  {auth},
  {hold}
}
```

För varje sektion finns en kronologisk mappning av förekommande fält och delfält 000-999 till egenskaper och klasser som definieras antingen i vårt vokabulär KBV eller den grupp termer som får ett marc prefix.

Marc termer får fält eller delfält där konvertering inte passar in i en BF2 form eller att behovet på något sätt är oklart eller att datan är tvetydig i förhållande till fältets betydelse.

Den vanligaste anledningen till marc-prefix är antingen att LC har slagit ihop fält så att de vid en återkonvertering inte går att separeras igen eller att de helt enkelt valt att inte försöka tolka (nac - no attempt to code, eller med en ignore för saker som inte har betydelse utanför MARC21).

Utöver ovanstående mappningar och mönster finns även postprocessing-steg. Dessa hanterar exempelvis datum/årsangivelse som behöver exporteras till flera fält (ex 008 och 264).

## Syntax i MARCFRAME

Det finns ett antal funktioner i MARCFRAME som är bra att känna till för de styr funktionalitet som repetition, inkludering av mönster osv. OBS! Dessa exempel är endast för illustration och är inte uppdaterade med aktuell mappning.

`about` gör att vi kan skapa kortformer av mer komplicerade eller nästlade entiteter.

`aboutAlias` används för att inkludera en about form.

`aboutEntity` Talar om vilket ting egenskapen sitter på, **thing** åsyftar instansen i bibliografisk data.

`aboutType` ???

`addLink` Talar om att den kan innehålla repeterande entiteter i det här fallet är repeterbar. Om entiteten endast ska kunna förekomma en gång används `link`. Huruvida ett MARC21 fält är repeterbart eller inte finns angivet i Formathandboken och LoC MARC21 specifikationen. 

`addProperty`(R) och `property` (NR) används för att tala om repeterbarhet för egenskaper.

`embedded` kan vara satt till *true* betyder att entiteten är ett sk strukturerat värde dvs att den bäddas in lokalt i posten utan att länkas till. Den typen av datastruktur kallas i RDF B-noder. De vanligaste typerna av embedded är Titlar, Identifikatorer och Anmärkningar.

`include` inkluderar det mönster som matchar värdet.

`inherit` skapar arv från angivet fält.

`link` se addLink.

`match` används för att matcha värden i indikatorer eller förekomst av delfält med en `when` konstruktion. Exempel 2. illustrerar att mappa rätt typ av agent eller verk i **$100** fältet beroende på vilken grupp som har det matchande värdet.

`property` se addProperty.

`punctuationChars` talar om att vilken interpunktion som bör tas bort och läggas åter i delfältet.

`splitValuePattern` med ett regexp för att i särskilda delfält kunna slita itu textsträngar, Exempel identifikatorer med efterföljande kvalifikatorer.

`splitValueProperties` talar om vilka egenskaper de två separerade grupperna ska hamna i. Exempel value, qualifier.

 `pendingResource` ger möjlighet att skapa en hierarkisk struktur av entiteter. 

`required: true` används för att tala om att ett fält inte ska konverteras till MARC21 om detta fält saknas. Man kan också med `supplementary: true` tala om att endast förekomsten av detta fält inte är tillräckligt för konvertering om det finns flera som kan anses required.

`resourceType` den entitet som ska skapas vid en addlink eller link.

`spec` används för att verifiera att indata och utdata är som vi förväntar oss finns sedan en spec som visar hur datan ser ut i `source` (indata) och `normalized` (utdata) dvs på utvägen, samt hur JSON-LD formen ska bli. Finns inte normalized med så ska utdatat vara detsamma som indatat, dvs source. Man kan också använda name för att namnge vad som testas.

`subfieldOrder` används för specificera fältordning vid konvertering till MARC21. Exempel `“a q c z”`. Om subfieldorder inte finns så tillämpas alfabetisk ordning.

`when` se match.

### Exempel 1. BIB 020
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
För **020** så finns en `include` vilket betyder att den hämtar ett mönster från nyckeln `“identifier”`
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
För identifier kan man även se att den inkluderar en idbase,
```
    "idbase": {
      "aboutEntity": "?thing",
      "addLink": "identifiedBy",
      "embedded": true
    },
```
Så sammantaget ser en 020 mappning ut såhär
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

### Exempel 2. BIB 100
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

## Källor för mappning

Informerande i mappningsarbetet har varit följande källor:

- [BIBFRAME 2 konverteringsspecifikationer](https://www.loc.gov/bibframe/)
- [Libris Formathandbok](https://www.kb.se/katalogisering/Formathandboken)
- [LC:s MARC21 specifikation](https://www.loc.gov/marc/)
- [OCLC](https://www.oclc.org/bibformats/en.html)
- Översättningar och konfigurationsfiler från gamla Libris system.
- Statistik över fältförekomst.

I MARCFRAME kan det förekomma anmärkningar från dessa källor.

| Anmärkning | Beskrivning |
| --- | --- |
| NOTE: | Generell anmärkning. |
| NOTE:local | Om användning från Libris formathandbok. |
| NOTE:LC | Anmärkning som rör Library of Congress MARC specifikation eller BF2 mappning. |
| NOTE:OCLC | OCLC definierade fält som kan förekomma i importerade poster. |



