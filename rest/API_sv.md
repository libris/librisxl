# Libris XL 'REST' API

**NOTERA:** Denna dokumentation är inte komplett, utan kan komma att ändras.
Den är i dagsläget enbart användbar för att testa integrationer, men räkna med
förändringar innan systemet tas i produktion.

## CRUD-API

Libris XL använder JSON-LD som dataformat och vi tillhandahåller ett API för
att skapa, läsa, uppdatera och radera poster. Läsoperationer kan utföras utan
autentisering, medan alla andra typer av operationer kräver en access-token.
Det finns två olika behörighetsnivåer för användare: en för att arbeta med
beståndsposter och en för katalogisering (som även tillåter arbete med
beståndsposter). Användarbehörigheter är knutna till sigel och en användare får
enbart arbeta med beståndsposter tillhörande ett sigel som användaren har
behörighet för. Mer information finns i stycket Autentisering i det här
dokumentet.

Vi har versionshantering av dokument och för detta sparar vi vilket sigel som
är ansvarig för förändringen. På grund av detta krävs att headern
`XL-Active-Sigel` är satt till den autentiserade användarens valda sigel.

Ett exempel på hur du kan arbeta med API:et finns i våra
[integrationstester](https://github.com/libris/lxl_api_tests).

### Att läsa en post

Du läser en post genom att skicka ett `GET`-anrop till postens URI (t.ex.
`https://libris-qa.kb.se/fnrbrgl`) med `Accept`-headern satt till t.ex.
`application/ld+json`. Standardinnehållstypen är `text/html` vilket ger en
renderad HTML-vy av posten och inte enbart underliggande data.

Svaret innehåller också en `ETag`-header, vars värde måste sättas i en
`If-Match`-header vid uppdatering av posten.


#### Exempel

```
$ curl -XGET -H "Accept: application/ld+json" https://libris-qa.kb.se/s93ns5h436dxqsh
{
  "@context": "/context.jsonld",
  "@graph": [
    ...
  ]
}
```


### Att skapa en post - Kräver autentisering

Du skapar en ny post genom att skicka ett `POST`-anrop till API:ets rot (`/`)
med åtminstone `Content-Type`-, `Authorization`- och `XL-Active-Sigel`-headern
satta.

API:et implementerar ett antal valideringssteg, till exempel för att förhindra
att duplicerade beståndsposter skapas. Om någon validering misslyckas svarar
API:et `400 Bad Request` med ett felmeddelande som förklarar problemet.

Ett lyckat anrop kommer resultera i ett `201 Created`-svar med den nya postens
URI satt i `Location`-headern.


#### Exempel

```
$ curl -XPOST -H "Content-Type: application/ld+json" \
    -H "Authorization: Bearer xxxx" -d@my_post.jsonld \
    https://libris-qa.kb.se/
...
```


### Att uppdatera en post - Kräver autentisering

Du kan uppdatera en befintlig post genom att skicka ett `PUT`-anrop till
postens URI (t.ex. `https://libris-qa.kb.se/fnrbrgl`) med åtminstone
`Content-Type`-, `Authorization`-, `If-Match`- och `XL-Active-Sigel`-headern
satta.

För att förhindra att en post som någon annan uppdaterat samtidigt skrivs över
måste `If-Match`-headern vara satt till det värde som fanns i `ETag`-headern
när dokumentet lästes upp. Om posten har ändrats kommer API:et svara med `409
Conflict`.

Du får inte ändra ID:t i posten, utan måste i stället radera ursprungsposten
och därefter skapa en ny post.

Ett lyckat anrop kommer resultera i ett `204 No Content`-svar, medan
misslyckade anrop kommer resultera i ett `400 Bad Request`-svar med medföljande
felmeddelande.


#### Exempel

```
$ curl -XPUT -H "Content-Type: application/ld+json" \
    -H "Authorization: Bearer xxxx" -d@my_updated_post.jsonld \
    https://libris-qa.kb.se/fnrbrgl
...
```


### Att radera en post - Kräver autentisering

Du kan radera en post genom att skicka ett `DELETE`-anrop till postens URI
(t.ex. `https://libris-qa.kb.se/fnrbrgl`) med `Authorization`- och
`XL-Active-Sigel`-headern satta.

Du kan enbart radera poster som inga andra poster länkar till. Det innebär att
om du till exempel vill radera en bibliografisk post kan du bara göra det om
det inte finns några beståndsposter kopplade till den. Om så är fallet kommer
API:et svara med `403 Forbidden`.

En lyckad borttagning kommer resultera i ett `204 No Content`-svar. Ytterligare
anrop till postens URI kommer resultera i ett `410 Gone`-svar.


#### Exempel

```
$ curl -XDELETE -H "Authorization: Bearer xxxx" \
    https://libris.kb.se/fnrbrgl
...
```


## Andra API-anrop

### `/find` - Sök i Libris-databasen

Detta anrop låter dig slå mot den interna Libris-databasen.

Standardoperatorn är `+` (`OCH`), vilket innebär att en sökning på `tove jansson` är
samma sak som en sökning på `tove +jansson`. `-` exkluderar söktermer, `|`
innebär `ELLER`, `*` används för prefixsökningar, `""` matchar hela frasen och
`()` används för att påverka operatorprioritet.


#### Parametrar

* `q` - Sökfrågan
* `_limit` - Max antal träffar att inkludera i resultatet, används för
  paginering. Standardvärdet är 200.
* `_offset` - Antal träffar att hoppa över i resultatet, används för
  paginering. Standardvärdet är 0.


#### Exempel

```
$ curl -XGET -H "Accept: application/ld+json" \
    https://libris-qa.kb.se/find\?q\=tove%20\(jansson\|lindgren\)\&_limit=2
...
```

### `/find?o` - Hitta alla poster som länkar till en viss post

Detta anrop låter dig hitta alla poster som länkar till en viss post i den interna Libris-databasen.

#### Parametrar

* `o` - ID för posten vars omvända relationer ska slås upp
* `_limit` - Max antal träffar att inkludera i resultatet, används för
  paginering. Standardvärdet är 200.
* `_offset` - Antal träffar att hoppa över i resultatet, används för
  paginering. Standardvärdet är 0.
  
  
#### Exempel

```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find?o=https://id.kb.se/country/vm&_limit=2'
...
```

### `/_remotesearch` - Sök i externa databaser  - Kräver autentisering

Detta anrop låter dig slå mot externa databaser.

#### Parametrar

* `q` - Sökfrågan
* `start` - Antal träffar att hoppa över i resultatet, används för paginering.
  Standardvärdet är 0.
* `n` - Max antal träffar att inkludera i resultatet, används för paginering.
  Standardvärdet är 10.
* `databases` - En kommaseparerad lista av databaser att söka i. Använd denna
  parameter ensam för att lista tillgängliga databaser.


#### Exempel

Lista tillgängliga databaser:

```
$ curl -XGET -H "Authorization: Bearer xxxx" 'https://libris-qa.kb.se/_remotesearch?databases=true'
[{"database":"AGRALIN","name":"Wageningen UR",
  "alternativeName":"Wageningen UR Library",
  "country":"Nederländerna",
  "comment":"Bibliografiska uppgifter om böcker och tidskrifter.",
  ...},
 ...
]
```

Sök i två externa databaser efter frasen "tove":

```
$ curl -XGET 'https://libris-qa.kb.se/_remotesearch?q=tove&databases=BIBSYS,DANBIB'
{"totalResults":{"BIBSYS":7336,"DANBIB":11991},"items":[...]}
```


### `/_convert` - Förhandsgranska MARC21-konvertering

Detta anrop låter dig förhandsgrandska hur ett JSON-LD-dokument kommer se ut
efter konvertering till MARC21.


#### Exempel

```
$ curl -XPOST -H "Content-Type: application/ld+json" \
    -H "Accept: application/x-marc-json" \
    -d@my_post.jsonld https://libris-qa.kb.se/_convert
...
```


### `/_findhold` - Hitta beståndsposter för en bibliografisk post

Detta anrop listar beståndsposterna som angivet sigel har för den specificerade
bibliografiska posten.


#### Parametrar

* `id` - Den bibliografiska postens ID (t.ex. https://libris-qa.kb.se/s93ns5h436dxqsh eller http://libris.kb.se/bib/1234)
* `library` - Sigel-ID (t.ex. https://libris.kb.se/library/SEK)


#### Exempel

```
$ curl -XGET 'https://libris-qa.kb.se/_findhold?id=https://libris-qa.kb.se/s93ns5h436dxqsh&library=https://libris.kb.se/library/SEK'
["https://libris-qa.kb.se/48h9kp894jm8kzz"]
$ curl -XGET 'https://libris-qa.kb.se/_findhold?id=http://libris.kb.se/bib/1234&library=https://libris.kb.se/library/SEK'
["https://libris-qa.kb.se/48h9kp894jm8kzz"]
```

### `/_merge` - Slå ihop två poster - Kräver autentisering

Detta anrop låter dig automatiskt slå ihop två bibliografiska poster. Detta är
användbart till exempel för att hantera duplicerade poster. Sammanslagningen
utgår från informationen som finns i posten som anges i `id1` och lägger om
möjligt till information från den andra posten. Vi ersätter aldrig information
från den första posten, utan utökar den enbart. Detta innebär att vi enbart
garanterar att information från den första posten kommer finnas med i den
sammanslagna posten.

Att ange två orelaterade poster i det här anropet är aldrig en bra idé.

Ett `GET`-anrop ger en förhandsgranskning av sammanslagningen, medan ett
`POST`-anrop skriver resultatet till databasen.

`POST`-anrop kräver en giltig access-token, som ska sättas i
`Authorization`-headern.


#### Parameters

* `id1` - Den första bibliografiska posten (t.ex. http://libris.kb.se/bib/1234)
* `id2` - Den andra bibliografiska posten (t.ex. http://libris.kb.se/bib/7149593)
* `promote_id2` - Bool för att indikera om `id2` ska användas i stället för
  `id1` som utgångspunkt för den resulterande posten (standardvärdet är
`false`).


#### Exempel

Förhandsgranska en sammanslagning av två orelaterade poster:

```
$ curl -XGET 'https://libris-qa.kb.se/_merge?id1=http://libris.kb.se/bib/1234&id2=http://libris.kb.se/bib/7149593'
...
```

**OBS:** Exemplet ovan är enbart användbart för att se hur sammanslagningen
fungerar. Slå aldrig ihop två orelaterade poster på det här sättet.


### `/_dependencies` - Lista en posts beroenden

#### Parametrar

* `id` - Den bibliografiska postens ID (t.ex. http://libris.kb.se/bib/1234)
* `relation` - Typ av relation (den här parametern är valfri och kan exkluderas)
* `reverse` - Bool för att indikera omvänd relation (standardvärdet är `false`)


#### Exempel

```
$ curl -XGET 'https://libris-qa.kb.se/_dependencies?id=http://libris.kb.se/bib/1234&relation=language'
["https://libris-qa.kb.se/zfpl8t8310mn9s2m"]
```


### `/_compilemarc` - Ladda ner en bibliografisk post i MARC21 med bestånds- och auktoritetsinformation

Detta anrop låter dig ladda ner en komplett bibliografisk post med bestånds-
och auktoritetsinformation i MARC21-format.


#### Parametrar

* `id` - Den bibliografiska postens ID (t.ex. http://libris.kb.se/bib/1234)
* `library` - Sigel-ID (t.ex. https://libris.kb.se/library/SEK)


#### Exempel

```
$ curl -XGET 'https://libris-qa.kb.se/_compilemarc?id=http://libris.kb.se/bib/1234&library=https://libris.kb.se/library/SEK'
01040cam a22002897  45000010005000000030008000050050017000130080041000300200015000710350015000860400008001010840013001090840014001220840016001360840017001520840018001690840017001872450047002042640038002513000021002897000029003107720121003398870107004608410051005678520020006188870112006381234SE-LIBR20180126135547.0011211s1971    xxu|||||||||||000 ||eng|   a0824711165  99900990307  aLi*  aUbb2sab  aUcef2sab  aUbb2kssb/5  aUcef2kssb/5  aUe.052kssb/5  aPpdc2kssb/500aWater and water pollution handbooknVol. 2 1aNew York :bMarcel Dekker ,c1971
 aS. 451-800bill.1 aCiaccio, Leonard L.4edt0 dNew York : Marcel Dekker, cop. 1971-1973w99000064967nn92ced. by L.L. CiacciotWater and water pollution handbook  a{"@id":"s93ns5h436dxqsh","modified":"2018-01-26T13:55:47.68+01:00","checksum":"12499440084"}2librisxl  5SEKax  ab150526||    |   1001||und|901128e4  5SEKbSEKhTEST2  5SEKa{"@id":"48h9kp894jm8kzz","modified":"2018-01-27T11:29:46.804+01:00","checksum":"-426929336"}2librisxl
```

## Autentisering

API:et använder [Libris Login](https://login.libris.kb.se) som OAuth2-provider.
Alla användare har ett personligt konto och för att autentisera användare
behöver din applikation vara registrerad som en OAuth2-klient.

Om autentiseringen är lyckad returneras en "bearer token", en "refresh token"
och en lista på användarens rättigheter. Denna lista kan (och bör) användas för
att låta användaren välja aktivt sigel, som krävs för att skapa, uppdatera och
radera poster (läs mer om detta i stycket om CRUD-API:et i detta dokument).

Den autentiserade användarens "bearer token" ska inkluderas i
`Authentication`-headern i API-anropen.

Om API:et inte kan validera informationen i headern kommer du få `401
Unauthorized` tillbaka, med ett meddelande som förklarar vad som gick fel
(ogiltig, utgången eller frånvarande token).
