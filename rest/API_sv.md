# Libris XL 'REST' API

De API:er som är dokumenterade här är inte versionerade och kan komma att
ändras i framtiden.

### Ordlista

`embellished` är en term som beskriver att en post levereras med inte bara sin egen data, utan även med relevant data som posten länkar till. Gränserna för precis vilka data som ligger i en viss sorts post, och vilka som brutits ut för att ligga för sig självt (och länkas till) varierar över tid. Generellt sett så bryts mer och mer data ut, eftersom datan då blir återanvändbar för flera poster. Det är ofta bra att be om poster `embellished` eftersom man då inte påverkas av dessa flyktiga gränsdragningar.

`framed` är en term som beskriver att den extra data som följer med en post på grund av länkningar ska monteras in på de ställen där datan används i JSON-LD-strukturen. Om data inte är `framed` så levereras den i stället som en lista av entiter.

`lens` är en term som beskriver en mekanism för att filtrera bort allt utom den viktigaste informationen i en post. Precis vad som filtreras bort beror på vilken typ av post/entitet man ber om. Det finns för tillfället tre värden för `lens` att välja mellan:
* `chip` är den mest filtrerade varianten. Används den, så är ofta det enda som blir kvar t ex entitetens namn (om ett sådant finns)
* `card` är mellan-nivån. Här inkluderas allting som finns på `chip`-nivån och viss ytterligare information. Det kan t ex vara variant-namn, eller identifikatorer.
* `none` innebär att ingen data filtreras bort.

## CRUD-API

Libris XL använder JSON-LD som dataformat och vi tillhandahåller ett API för
att skapa, läsa, uppdatera och radera poster. Läsoperationer kan utföras utan
autentisering, medan alla andra typer av operationer kräver en access-token.

### Att läsa en post

En post kan läsas genom att skicka ett `GET`-anrop till postens URI (t.ex.
`https://libris-qa.kb.se/<post-id>`) med `Accept`-headern satt till t.ex.
`application/ld+json`. Standardinnehållstypen är `text/html` vilket ger en
renderad HTML-vy av posten och inte enbart underliggande data.

Formatet på data i svaret kan styras via parametrar.

#### Parametrar
Alla parametrar är valfria och kan utelämnas.
* `embellished` - `true` eller `false`. Standardvärdet är `true`.
* `framed` - `true` eller `false`. 
  * Standardvärdet är `false` för `Content-Type: application/ld+json`. 
  * Standardvärdet är `true` för  `Content-Type: application/json`.
* `lens` - `chip`, `card` eller `none`. Standardvärdet är `none`. Om `lens` är `chip` eller `card` blir `framed` alltid `true`.

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

```
$ curl -XGET -H "Accept: application/ld+json" https://libris-qa.kb.se/s93ns5h436dxqsh?embellished=false&lens=chip
{
  "@context": "/context.jsonld",
  "@graph": [
    ...
  ]
}
```

### Profile Negotiation

För att få data i en annan variant (genom att använda ett specifikt
urval av RDF-vokabulär) stöder vi en form av "profile negotiation".

(Observera att Profile Negotiation ännu (2024) inte är en standard. Se
[IETF Internet Draft on Profile Negotiation](https://profilenegotiation.github.io/I-D-Profile-Negotiation/I-D-Profile-Negotiation.html)
och
[W3C Working Draft on Content Negotiation by Profile](https://www.w3.org/TR/dx-prof-conneg/)
för detaljer.)

#### Profiler tillgängliga just nu

Se [målkontexterna i libris-definitions-repot](https://github.com/libris/definitions/tree/develop/sys/context/target).

#### Profilerade datavyer

Med content negotiation plus parametrar:
```
$ curl -s -H'Accept: text/turtle' "https://libris-qa.kb.se/fxql7jqr38b1dkf?profile=https://id.kb.se/sys/context/target/sdo-w3c"

$ curl -s -H'Accept: text/turtle' "https://id-qa.kb.se/relator/contributor?profile=https://id.kb.se/sys/context/target/bibo-w3c"

$ curl -s -H'Accept: text/turtle' "https://libris-qa.kb.se/fxql7jqr38b1dkf?profile=https://id.kb.se/sys/context/target/bibo-w3c"
```

Kombination av parametrar:
```
$ curl -s -H'Accept: text/turtle' "https://libris-qa.kb.se/fxql7jqr38b1dkf?profile=https://id.kb.se/sys/context/target/sdo-w3c&embellished=false"
```

Genom att använda enbart headers för negotiation:
```
$ curl -s -H 'Accept: application/trig' -H 'Accept-Profile: <https://id.kb.se/sys/context/target/loc-w3c-sdo>' \
      http://libris-qa.kb.se/fxql7jqr38b1dkf
```

### Content Negotiation

För att få data i olika representationer stöder vi [content negotiation](https://developer.mozilla.org/en-US/docs/Web/HTTP/Content_negotiation).
Just nu stöds följande:

* `application/ld+json` ([JSON-LD](https://www.w3.org/TR/json-ld11/))
* `application/json` (JSON)
* `text/turtle` ([Turtle](https://www.w3.org/TR/turtle/))
* `application/trig` ([TriG](https://www.w3.org/TR/trig/))
* `application/rdf+xml` ([RDF/XML](https://www.w3.org/TR/rdf-syntax-grammar/))

```
$ curl -s -H'Accept: application/ld+json' https://libris-qa.kb.se/s93ns5h436dxqsh

$ curl -s -H'Accept: application/json' https://libris-qa.kb.se/s93ns5h436dxqsh

$ curl -s -H'Accept: text/turtle' https://libris-qa.kb.se/s93ns5h436dxqsh

$ curl -s -H'Accept: application/trig' https://libris-qa.kb.se/s93ns5h436dxqsh

$ curl -s -H'Accept: application/rdf+xml' https://libris-qa.kb.se/s93ns5h436dxqsh
```

För bekvämlighetens skull lägga till `/data.<filändelse>` för att få en viss representation
utan att behöva specificera den med `Accept`-headern (i det här fallet ignoreras `Accept`-headern):

```
# JSON-LD
$ curl -s https://libris-qa.kb.se/s93ns5h436dxqsh/data.jsonld

# JSON
$ curl -s https://libris-qa.kb.se/s93ns5h436dxqsh/data.json

# Turtle
$ curl -s https://libris-qa.kb.se/s93ns5h436dxqsh/data.ttl

# TriG
$ curl -s https://libris-qa.kb.se/s93ns5h436dxqsh/data.trig

# RDF/XML
$ curl -s https://libris-qa.kb.se/s93ns5h436dxqsh/data.rdf
```

## Anrop som kräver autentisering – skapa, uppdatera och radera

För att göra anrop mot våra API:er som kräver autentisering behöver en
oauth-klient registreras i Libris. För att registrera en klient behöver
ni kontakta Libris kundservice som återkommer med den nya klientens id och
klienthemlighet samt en instruktion för hur dessa ska användas för att erhålla
en bearertoken. Det är denna bearertoken som det hänvisas till i exemplen nedan.
Utdelade uppgifter gäller antingen för testmiljöerna eller för produktionsmiljön beroende
på integrationens karaktär och användarens behov.

Det finns två olika behörighetsnivåer för klienten: en för att arbeta med
beståndsposter och en för att arbeta med både bibliografiska poster och beståndsposter.
Klienten är knuten till en eller flera sigel, och det är enbart beståndsposter
tillhörande den eller dessa sigel som kan skapas, ändras eller tas bort i exemplen
som följer.

### Hämta ut en bearertoken

När man har fått ett klientid och klienthemlighet ifrån kundtjänst gör man följande för att hämta
en beartoken:

```
$ curl -X POST -d 'client_id=<Ert klientid>&client_secret=<Er klienthemlighet>&grant_type=client_credentials' https://login.libris.kb.se/oauth/token'
```
För att hämta ut en token som gäller för testmiljöerna anropas istället `https://login-stg.libris.kb.se/oauth/token`.

Svaret på anropet bör se ut ungefär som följande:

```
{"access_token": "tU77KXIxxxxxxxKh5qxqgxsS", "expires_in": 36000, "token_type": "Bearer", "scope": "read write", "app_version": "1.5.0"}
```
Utifrån svaret på anropet förväntas ni ta ut er access_token och skicka med den vid varje
anrop som kräver autentisering.

### Skapa

En ny post kan skapas genom att skicka ett `POST`-anrop till `/data`
med åtminstone `Content-Type`-, `Authorization`- och `XL-Active-Sigel`-headern
satta.

API:et implementerar ett antal valideringssteg, till exempel för att förhindra
att duplicerade beståndsposter skapas. Om någon validering misslyckas svarar
API:et `400 Bad Request` med ett felmeddelande som förklarar problemet.

Observera att ett tillfälligt `@id` **måste** specificeras på flera ställen i
dokumentet du skickar:

* Tillfälligt `@id` i Record
* Tillfälligt `mainEntity.@id` i Record
* Tillfälligt `@id` i Thing ("saken")

Till exempel:

```
{"@graph":[{"@id":"https://id.kb.se/TEMPID","@type":"Record","mainEntity":{"@id":"https://id.kb.se/TEMPID#it"}},{"@id":"https://id.kb.se/TEMPID#it","@type":"Person","familyName":"Testing"}]}
```

Ett lyckat anrop kommer resultera i ett `201 Created`-svar med den nya postens
URI satt i `Location`-headern.


#### Exempel

```
$ curl -XPOST -H 'Content-Type: application/ld+json' \
    -H 'Authorization: Bearer <token>' \
    -H 'XL-Active-Sigel: <sigel>' \
    -d@min_post.jsonld \
    https://libris-qa.kb.se/data
```
* `<token>` - En aktiv och giltig bearertoken (t.ex. hW3IHc9PexxxFP2IkAAbqKvjLbW4thQ)
* `<sigel>` - En sigel som din oauth-klient är knuten till (t.ex. Doro)

### Uppdatera

En befintlig post kan uppdateras genom att skicka ett `PUT`-anrop till
postens URI (t.ex. `https://libris-qa.kb.se/s93ns5h436dxqsh`) med åtminstone
`Content-Type`-, `Authorization`-, `If-Match`- och `XL-Active-Sigel`-headern
satta.

För att göra ett `PUT`-anrop behöver man skicka med en `If-Match`-header som
innehåller en`ETag`. Detta för att förhindra samtidiga uppdateringar av ett
dokument. Det värde som ska fyllas i kan hämtas från `ETag`-headern i det svar
som returneras när man gör ett `GET`-anrop mot en viss post. Om posten hunnit
ändras av någon annan efter det att `ETag`:en hämtats kommer API:et svara med
`409 Conflict`.

Det går inte att uppdatera ID:t för en given post. Om ett nytt ID önskas behöver
posten tas bort och en ny skapas i dess ställe.

Ett lyckat anrop kommer resultera i ett `204 No Content`-svar, medan
misslyckade anrop kommer resultera i ett `400 Bad Request`-svar med medföljande
felmeddelande.


#### Exempel

```
$ curl -XPUT -H 'Content-Type: application/ld+json' \
    -H 'If-Match: <etag>' \
    -H 'Authorization: Bearer <token>' \
    -H 'XL-Active-Sigel: <sigel>' \
    -d@min_uppdaterade_post.jsonld \
    https://libris-qa.kb.se/<id>
```
* `<etag>` - Postens ETag (t.ex. 1821177452)
* `<token>` - En aktiv och giltig bearertoken (t.ex. hW3IHc9PexxxFP2IkAAbqKvjLbW4thQ)
* `<sigel>` - Den sigel som din oauth-klient är knuten till (t.ex. S)
* `<id>` - Postens ID (t.ex. s93ns5h436dxqsh)

### Radera

Du kan radera en post genom att skicka ett `DELETE`-anrop till postens URI
(t.ex. `https://libris-qa.kb.se/<post-id>`) med `Authorization`- och
`XL-Active-Sigel`-headern satta.

Du kan enbart radera poster som inga andra poster länkar till. Det innebär att
om du till exempel vill radera en bibliografisk post kan du bara göra det om
det inte finns några beståndsposter kopplade till den. Om så är fallet kommer
API:et svara med `403 Forbidden`.

En lyckad borttagning kommer resultera i ett `204 No Content`-svar. Ytterligare
anrop till postens URI kommer resultera i ett `410 Gone`-svar.


#### Exempel

```
$ curl -XDELETE
    -H 'Authorization: Bearer <token>' \
    -H 'XL-Active-Sigel: <sigel>' \
    https://libris-qa.kb.se/<id>
```
* `<token>` - En aktiv och giltig bearertoken (t.ex. hW3IHc9PexxxFP2IkAAbqKvjLbW4thQ)
* `<sigel>` - Den sigel som din oauth-klient är knuten till (t.ex. T)
* `<id>` - Postens ID (t.ex. s93ns5h436dxqsh)

## Andra API-anrop

### `/find` - Sök i Libris-databasen

Detta anrop låter dig slå mot den interna Libris-databasen.

Standardoperatorn är `+` (`OCH`), vilket innebär att en sökning på `tove jansson` är
samma sak som en sökning på `tove +jansson`. `-` exkluderar söktermer, `|`
innebär `ELLER`, `*` används för prefixsökningar, `""` matchar hela frasen och
`()` används för att påverka operatorprioritet.


#### Parametrar

* `q` - Sökfrågan.
* `o` - Hitta endast poster som länkar till detta ID.
* `_limit` - Max antal träffar att inkludera i resultatet, används för
  paginering. Standardvärdet är 200.
* `_offset` - Antal träffar att hoppa över i resultatet, används för
  paginering. Standardvärdet är 0.
  
Sökningen kan filtreras på värdet på egenskaper i posten. Samma egenskap kan anges flera gånger genom
att upprepa parametern eller genom att komma-separera värdena. Om olika egenskaper anges innebär det 
`OCH`. Om samma egenskap anges flera gånger innebär det `ELLER`, såvida inte prefixet `and-` används.
Det går att kombinera olika egenskaper med `ELLER` genom att använda prefixet `or-`.

* `<egenskap>` - Egenskapen har värdet.
* `not-<egenskap>` - Egenskapen har inte värdet.
* `or-<egenskap>` - Kombinera filter för olika egenskaper med `ELLER` istället för `OCH`.
* `and-<field>` - Kombinera filter för _samma_ egenskap med `OCH` istället för `ELLER`.
* `exists-<egenskap>` - Egenskapen existerar. Ange ett booleskt värde, d.v.s. `true` eller `false`.
* `min-<egenskap>` - Värdet är större eller lika med.
* `minEx-<egenskap>` - Värdet är större än (Ex står för "Exclusive").
* `max-<egenskap>` - Värdet är mindre eller lika med.
* `maxEx-<egenskap>` - Värdet är mindre än.
* `matches-<egenskap>` - Värdet matchar (se datumsökning och "inkludera underordnade termer"-sökning nedan).
* `and-matches-<field>` - Kombinera flera `matches`-filter för _samma_ fält med `OCH` istället för `ELLER`.

 Filter-uttryck                                        | Filter-parametrar    
-------------------------------------------------------|-----------------------                 
 a är x `ELLER` a är y...                              | `a=x&a=y...`
 a är x `OCH` a är `INTE` y...                         | `a=x&not-a=y...`
 a är x `OCH` a är y...                                | `and-a=x&and-a=y...`
 a är x `OCH` b är y `OCH` c är z...                   | `a=x&b=y&c=z...`
 a är x `ELLER` b är y...                              | `or-a=x&or-b=y...`          
 (a är x `ELLER` b är y) `OCH` c är z...               | `or-a=x&or-b=y&c=z...`  
 (a är x `ELLER` b är y) `OCH` (c är z `ELLER` d är q) | Inte möjligt. Kan bara ange en grupp med `or-`.                    

För egenskaper som är av typen datum (`meta.created`, `meta.modified` och `meta.generationDate`)
kan värdet anges på följande format:

| Format                  | Upplösning | Exempel               |
|-------------------------|------------|-----------------------|
| `ÅÅÅÅ`                  | År         | `2020`                |
| `ÅÅÅÅ-MM`               | Månad      | `2020-04`             |
| `ÅÅÅÅ-MM-DD`            | Dag        | `2020-04-01`          |
| `ÅÅÅÅ-MM-DD'T'HH`       | Timme      | `2020-04-01T12`       |
| `ÅÅÅÅ-MM-DD'T'HH:mm`    | Minut      | `2020-04-01T12:15`    |
| `ÅÅÅÅ-MM-DD'T'HH:mm:ss` | Sekund     | `2020-04-01T12:15:10` |
| `ÅÅÅÅ-'W'VV`            | Vecka      | `2020-W04`            |

#### Exempel
```
$ curl -XGET -H "Accept: application/ld+json" \
    https://libris-qa.kb.se/find\?q\=tove%20\(jansson\|lindgren\)\&_limit=2
...
```

#### Exempel

Länkar till country/Vietnam.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find?o=https://id.kb.se/country/vm&_limit=2'
...
```

#### Exempel

Har medietyp (mediaType) men inte bärartyp (carrierType).
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find?exists-mediaType=true&exists-carrierType=false'
...
```

#### Exempel

Utgiven på 1760-talet.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?min-publication.year=1760&maxEx-publication.year=1770&_limit=5'
...
```

#### Exempel

Noterad musik utgiven på 1930- eller 1950-talet.
```
$ curl -XGET -H "Accept: application/ld+json" -G \
    'https://libris-qa.kb.se/find.jsonld' \
    -d instanceOf.@type=NotatedMusic \
    -d min-publication.year=1930 \
    -d max-publication.year=1939 \
    -d min-publication.year=1950 \
    -d max-publication.year=1959 \
    -d _limit=5
...
```

#### Exempel

Katalogiserad av sigel "S" vecka åtta eller tio 2018.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?meta.descriptionCreator.@id=https://libris.kb.se/library/S&matches-meta.created=2018-W08,2018-W10&_limit=2'
...
```

#### Exempel

Innehåller 'Aniara' och har ett bestånd med sigel APP1.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?q=Aniara&@reverse.itemOf.heldBy.@id=https://libris.kb.se/library/APP1'
...
```

#### Exempel

Har ämne term/sao/Monster.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?instanceOf.subject.@id=https://id.kb.se/term/sao/Monster'
...
```

#### Exempel

Har ämne term/sao/Monster _eller_ term/sao/Magi.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?instanceOf.subject.@id=https://id.kb.se/term/sao/Monster&instanceOf.subject.@id=https://id.kb.se/term/sao/Magi'
...
```

#### Exempel

Har ämne term/sao/Monster _och_ term/sao/Magi.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?and-instanceOf.subject.@id=https://id.kb.se/term/sao/Monster&and-instanceOf.subject.@id=https://id.kb.se/term/sao/Magi'
...
```

#### Exempel

Har ämne term/sao/Monster _och_ term/sao/Magi men _inte_ term/sao/Trollkarlar,
och har genre/form term/saogf/Fantasy.
```
$ curl -XGET -H "Accept: application/ld+json" -G \
    'https://libris-qa.kb.se/find.jsonld' \
    -d and-instanceOf.subject.@id=https://id.kb.se/term/sao/Monster \
    -d and-instanceOf.subject.@id=https://id.kb.se/term/sao/Magi \
    -d not-instanceOf.subject.@id=https://id.kb.se/term/sao/Trollkarlar \
    -d instanceOf.genreForm.@id=https://id.kb.se/term/saogf/Fantasy
...
```

#### Exempel

Har ämne term/sao/Monster (eller en underordnad term, t.ex. Drakar, Gorgoner).
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?matches-instanceOf.subject.@id=https://id.kb.se/term/sao/Monster'
...
```

#### Exempel

Har ämne term/sao/Monster (eller en underordnad term, t.ex. Drakar, Gorgoner), _och_ term/sao/Magi (eller en underordnad term,
t.ex. Amuletter, Häxeri).
```
$ curl -XGET -H "Accept: application/ld+json" -G \
    'https://libris-qa.kb.se/find.jsonld' \
    -d and-matches-instanceOf.subject.@id=https://id.kb.se/term/sao/Monster \
    -d and-matches-instanceOf.subject.@id=https://id.kb.se/term/sao/Magi
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
och auktoritetsinformation i MARC21-format. Om den kompletta posten ska innehålla beståndsinformation så måste en exportprofil först ha registrerats för ditt sigel-ID. Kontakta kundtjänst i fall du behöver registrera en sådan profil.


#### Parametrar

* `id` - Den bibliografiska postens ID (t.ex. http://libris.kb.se/bib/1234)
* `library` - Sigel-ID (t.ex. https://libris.kb.se/library/SEK)


#### Exempel

```
$ curl -XGET 'https://libris-qa.kb.se/_compilemarc?id=http://libris.kb.se/bib/1234&library=https://libris.kb.se/library/SEK'
01040cam a22002897  45000010005000000030008000050050017000130080041000300200015000710350015000860400008001010840013001090840014001220840016001360840017001520840018001690840017001872450047002042640038002513000021002897000029003107720121003398870107004608410051005678520020006188870112006381234SE-LIBR20180126135547.0011211s1971    xxu|||||||||||000 ||eng|   a0824711165  99900990307  aLi*  aUbb2sab  aUcef2sab  aUbb2kssb/5  aUcef2kssb/5  aUe.052kssb/5  aPpdc2kssb/500aWater and water pollution handbooknVol. 2 1aNew York :bMarcel Dekker ,c1971
 aS. 451-800bill.1 aCiaccio, Leonard L.4edt0 dNew York : Marcel Dekker, cop. 1971-1973w99000064967nn92ced. by L.L. CiacciotWater and water pollution handbook  a{"@id":"s93ns5h436dxqsh","modified":"2018-01-26T13:55:47.68+01:00","checksum":"12499440084"}2librisxl  5SEKax  ab150526||    |   1001||und|901128e4  5SEKbSEKhTEST2  5SEKa{"@id":"48h9kp894jm8kzz","modified":"2018-01-27T11:29:46.804+01:00","checksum":"-426929336"}2librisxl
```
