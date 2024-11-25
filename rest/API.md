# Libris XL 'REST' API

The APIs in this document are not versioned and hence this interface might
change in the future.

### Glossary

`embellished` is a term used to specify that a record should be delivered with not only it's own data, but also with any relevant data that the record links to. The boundaries for where any one record ends and another begins shift over time. Generally the trend is that more and more data is broken out of the records that used to hold them, into new records of their own, in order to make that data reusable for many records. It is often a good idea to request your data `embellished` as that means you will not be affected by these shifting boundaries.

`framed` is a term meaning that any extra data delivered with your requested record (generally due to linking) should be embedded in every location where it is referenced within the JSON-LD-structure. If you request data that is not `framed` it is instead delivered as a list of separate entities.

`lens` is a term for selecting how much of a records information you want filtered out of the response. Exactly which information is filtered depends on the type of record/entity you are requesting. At present there are three levels of filtering available:
* `chip` is the most filtered variant. On this level, often the only thing remaining will be an entities name (if there is one).
* `card` is the mid-level of filtering, it includes whatever was at the `chip`-level and a little bit more. For example it could be variant-names and other forms of identifiers.
* `none` means no data is filtered.

## CRUD API

Libris XL uses JSON-LD as data format, and we provide an API to create, read,
update, and delete records. Read operations are available without authentication,
but all other requests require an access token.

### Reading a record

A record can be read by sending a `GET` request to the record's URI (e.g.
`https://libris-qa.kb.se/<record-id>`) with the `Accept` header set to e.g.
`application/ld+json`. The default content type is `text/html`, which would
give you an HTML rendering of the record and not just the underlying data.

#### Parameters
All parameters are optional and can be left out.
* `embellished` - `true` or `false`. Default is `true`.
* `framed` - `true` or `false`. 
  * Default `false` for `Content-Type: application/ld+json`. 
  * Default `true` for  `Content-Type: application/json`.
* `lens` - `chip`, `card` or `none`. Default is `none`. If value is `chip` or `card` this implies `framed=true`.


#### Examples

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

To get data in a different flavour (using a specific selection of RDF
vocabularies), we support a form of profile negotiation.

(Note: As of 2024, Profile Negotiation is not yet a standard. See the
[IETF Internet Draft on Profile Negotiation](https://profilenegotiation.github.io/I-D-Profile-Negotiation/I-D-Profile-Negotiation.html)
and
[W3C Working Draft on Content Negotiation by Profile](https://www.w3.org/TR/dx-prof-conneg/)
for details.)

#### Currently Defined Profiles

See the [target contexts in the libris definitions repository](https://github.com/libris/definitions/tree/develop/sys/context/target).

#### Profiled Data Views

Using content negotation plus parameters:
```
$ curl -s -H'Accept: text/turtle' "https://libris-qa.kb.se/fxql7jqr38b1dkf?profile=https://id.kb.se/sys/context/target/sdo-w3c"

$ curl -s -H'Accept: text/turtle' "https://id-qa.kb.se/relator/contributor?profile=https://id.kb.se/sys/context/target/bibo-w3c"

$ curl -s -H'Accept: text/turtle' "https://libris-qa.kb.se/fxql7jqr38b1dkf?profile=https://id.kb.se/sys/context/target/bibo-w3c"
```

Combining parameters:
```
$ curl -s -H'Accept: text/turtle' "https://libris-qa.kb.se/fxql7jqr38b1dkf?profile=https://id.kb.se/sys/context/target/sdo-w3c&embellished=false"
```

Using only headers for negotation:
```
$ curl -s -H 'Accept: application/trig' -H 'Accept-Profile: <https://id.kb.se/sys/context/target/loc-w3c-sdo>' \
      http://libris-qa.kb.se/fxql7jqr38b1dkf
```

### Content Negotiation

To get data in different representations, we support [content negotiation](https://developer.mozilla.org/en-US/docs/Web/HTTP/Content_negotiation).
Currently supported:

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

For convenience, you can add `/data.<extension>` to get a certain representation
without having to specify it with `Accept` (in this case, the `Accept` header is ignored):

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

## Requests that require authentication – create, update and remove

In order to make requests that require authentication, an oauth client has to be registered
in Libris. To register a client, contact Libris customer service and they will provide you
with a new set of credentials (client id and client secret) as well as an instruction on
how these can be used to obtain a bearer token. This bearer token is referred to in the examples
below. The provided credentials work either for the Libris test environments or the production environment, depending 
on the character of the integration as well as the needs of the user.

There are two different permission levels for the client: one for working with holdings
and one for working with both holdings and bibliographic records. The client is connected
to one or several sigels. Only holdings connected to any of these sigels can be created, updated or
removed in the following examples.

### Obtain a bearertoken

Once you have obtained a client id and a client secret from Libris customer service, do the
following to obtain a bearertoken:

```
$ curl -X POST -d 'client_id=<Your client id>&client_secret=<Your client secret>&grant_type=client_credentials' https://login.libris.kb.se/oauth/token'
```
To fetch a token that is valid for the test environments, adjust the token endpoint to `https://login-stg.libris.kb.se/oauth/token`.

The response to the above request should look something like: 

```
{"access_token": "tU77KXIxxxxxxxKh5qxqgxsS", "expires_in": 36000, "token_type": "Bearer", "scope": "read write", "app_version": "1.5.0"}
```

From that response you are expected to extract your access_token and pass it along with each
subsequent request that requires authentication.

### Create

A new record is created by sending a `POST` request to `/data` with at least the
`Content-Type`, `Authorization`, and `XL-Active-Sigel` headers set.

There are some checks in place, e.g. in order to prevent creation of duplicate
holding records, and to these requests the API responds with a `400 Bad Request`
with an error message explaining the issue.

Note that a temporary `@id` **must** be set in a few places in the document you send:

* Temporary `@id` in Record
* Temporary `mainEntity.@id` in Record
* Temporary `@id` in Thing

For example:

```
{"@graph":[{"@id":"https://id.kb.se/TEMPID","@type":"Record","mainEntity":{"@id":"https://id.kb.se/TEMPID#it"}},{"@id":"https://id.kb.se/TEMPID#it","@type":"Person","familyName":"Testing"}]}
```

A successful creation will get a `201 Created` response with a `Location`
header containing the URI of the new record.


#### Example

```
$ curl -XPOST -H 'Content-Type: application/ld+json' \
    -H 'Authorization: Bearer <token>' \ 
    -H 'XL-Active-Sigel: <sigel>' \
    -d@my_record.jsonld \
    https://libris-qa.kb.se/data
```
* `<token>` - An active and valid bearer token (e.g. hW3IHc9PexxxFP2IkAAbqKvjLbW4thQ)
* `<sigel>` - A sigel that is connected to your oauth client (e.g. Doro)

### Update

An existing record can be updated by sending a `PUT` request to the record URI (e.g.
`https://libris-qa.kb.se/s93ns5h436dxqsh`) with at least the `Content-Type`,
`Authorization`, `If-Match`, and `XL-Active-Sigel` headers set.

To prevent accidentally overwriting a record modified by someone else, you need
to set the record's `ETag` in the `If-Match` header. The `ETag` value can be obtained
from the `ETag` header in the response of a `GET` request from a specific record.
If the record has been modified by someone else since the `ETag` was fetched, you will
receive a `409 Conflict` response.

You are not allowed to change the ID of a record, for that you must instead first
delete the original record and then create the replacement.

A successful update will get a `204 No Content` response, and invalid requests
will get a `400 Bad Request` response, with an error message.


#### Example

```
$ curl -XPUT -H 'Content-Type: application/ld+json' \
    -H 'If-Match: <etag>' \
    -H 'Authorization: Bearer <token>' \ 
    -H 'XL-Active-Sigel: <sigel>' \
    -d@my_updated_record.jsonld \
    https://libris-qa.kb.se/<id>
```
* `<etag>` - The record's ETag (e.g. 1821177452)
* `<token>` - An active and valid bearer token (e.g. hW3IHc9PexxxFP2IkAAbqKvjLbW4thQ)
* `<sigel>` - A sigel that is connected to your oauth client (e.g. S)
* `<id>` - Record ID (e.g. s93ns5h436dxqsh)

### Delete

You delete a record by sending a `DELETE` request to the record URI (e.g.
`https://libris-qa.kb.se/<record-id>`) with the `Authorization` and
`XL-Active-Sigel` headers set.

You can only delete records that are not linked to by other records. That is, if
you want to delete a bibliographic record, you can only do so if there are no
holding records related to it. If there are holding records related to it, the API
will respond with a `403 Forbidden`.

A successful deletion will get a `204 No Content` response. Any subsequent
requests to the record's URI will get a `410 Gone` response.


#### Example

```
$ curl -XDELETE 
    -H 'Authorization: Bearer <token>' \
    -H 'XL-Active-Sigel: <sigel>' \
    https://libris-qa.kb.se/<id>
```
* `<token>` - An active and valid bearer token (e.g. hW3IHc9PexxxFP2IkAAbqKvjLbW4thQ)
* `<sigel>` - A sigel that is connected to your oauth client (e.g. T)
* `<id>` - Record ID (e.g. s93ns5h436dxqsh)

## Other API endpoints

### `/find` - Search the Libris database

This endpoint allows you to query the internal Libris database.

The default operator is `+` (`AND`), which means that a search for `tove
jansson` is equivalent to a search for `tove +jansson`. `-` excludes terms, `|`
means `OR`, `*` is used for prefix queries, `""` matches the whole phrase, and
`()` is used for operator precedence.

#### Parameters

* `q` - Search query.
* `o` - Only find records that link to this ID.
* `_limit` - Max number of hits to include in result, used for pagination.
  Default is 200.
* `_offset` - Number of hits to skip in the result, used for pagination.
  Default is 0.

Records can be filtered on field values. Specifying multiple values for the same field can be done
by repeating the parameter or by giving a comma-separated list as value. Specifying multiple fields 
means `AND`. Multiple values for the same field means `OR`, unless you use the prefix `and-`.
It is possible to combine multiple fields with `OR` by using the prefix `or-`.

* `<field>` - The record has this value for `field`.
* `not-<field>` - The record does not have this value for `field`.
* `or-<field>` - Combine filters for multiple fields with `OR` instead of `AND`.
* `and-<field>` - Combine multiple filters for the _same_ field with `AND` instead of `OR`.
* `exists-<field>` - The field exists in the record. Value should be `true` or `false`.
* `min-<field>` - Greater or equal to.
* `minEx-<field>` - Greater than (exclusive minimum).
* `max-<field>` - Less or equal to.
* `maxEx-<field>` - Less than.
* `matches-<field>` - Value is matching (see date-search and "include narrower terms" search below).
* `and-matches-<field>` - Combine multiple `matches-` filters for the _same_ field with `AND` instead of `OR`.

Filter-expression                                     | Filter-parameters
------------------------------------------------------|----------------------------------------------
a is x `OR` a is y...                                 | `a=x&a=y...`
a is x `AND` a is `NOT` y...                          | `a=x&not-a=y...`
a is x `AND` a is y...                                | `and-a=x&and-a=y...`
a is x `AND` b is y `AND` c is z...                   | `a=x&b=y&c=z...`
a is x `OR` b is y...                                 | `or-a=x&or-b=y...`
(a is x `OR` b is y) `AND` c is z...                  | `or-a=x&or-b=y&c=z...`
(a is x `OR` b is y) `AND` (c is z `OR` d is q)       | Not possible. Can only specify one group with `or-`.

For fields of type date (`meta.created`, `meta.modified` and `meta.generationDate`)
the following formats can be used for value:

| Format                  | Precision  | Example               |
|-------------------------|------------|-----------------------|
| `ÅÅÅÅ`                  | Year       | `2020`                |
| `ÅÅÅÅ-MM`               | Month      | `2020-04`             |
| `ÅÅÅÅ-MM-DD`            | Day        | `2020-04-01`          |
| `ÅÅÅÅ-MM-DD'T'HH`       | Hour       | `2020-04-01T12`       |
| `ÅÅÅÅ-MM-DD'T'HH:mm`    | Minute     | `2020-04-01T12:15`    |
| `ÅÅÅÅ-MM-DD'T'HH:mm:ss` | Second     | `2020-04-01T12:15:10` |
| `ÅÅÅÅ-'W'VV`            | Week       | `2020-W04`            |

#### Example

```
$ curl -XGET -H "Accept: application/ld+json" \
    https://libris-qa.kb.se/find\?q\=tove%20\(jansson\|lindgren\)\&_limit=2
...
```

#### Example

Linking to country/Vietnam.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find?o=https://id.kb.se/country/vm&_limit=2'
...
```

#### Example

Has mediaType but not carrierType.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find?exists-mediaType=true&exists-carrierType=false'
...
```

#### Example

Published in the 1760s.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?min-publication.year=1760&maxEx-publication.year=1770&_limit=5'
...
```

#### Example

Notated music published in the 1930s or 1950s.
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

#### Example

Catalogued by sigel "S" week eight or week ten 2018.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?meta.descriptionCreator.@id=https://libris.kb.se/library/S&matches-meta.created=2018-W08,2018-W10&_limit=2'
...
```

#### Example

Containing 'Aniara' and held by sigel APP1.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?q=Aniara&@reverse.itemOf.heldBy.@id=https://libris.kb.se/library/APP1'
...
```

#### Example

Has subject term/sao/Monster.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?instanceOf.subject.@id=https://id.kb.se/term/sao/Monster'
...
```

#### Example

Has subject term/sao/Monster _or_ subject term/sao/Magi.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?instanceOf.subject.@id=https://id.kb.se/term/sao/Monster&instanceOf.subject.@id=https://id.kb.se/term/sao/Magi'
...
```

#### Example

Has subject term/sao/Monster _and_ subject term/sao/Magi.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?and-instanceOf.subject.@id=https://id.kb.se/term/sao/Monster&and-instanceOf.subject.@id=https://id.kb.se/term/sao/Magi'
...
```

#### Example

Has subject term/sao/Monster _and_ subject term/sao/Magi but _not_ term/sao/Trollkarlar,
and has genreForm term/saogf/Fantasy.
```
$ curl -XGET -H "Accept: application/ld+json" -G \
    'https://libris-qa.kb.se/find.jsonld' \
    -d and-instanceOf.subject.@id=https://id.kb.se/term/sao/Monster \
    -d and-instanceOf.subject.@id=https://id.kb.se/term/sao/Magi \
    -d not-instanceOf.subject.@id=https://id.kb.se/term/sao/Trollkarlar \
    -d instanceOf.genreForm.@id=https://id.kb.se/term/saogf/Fantasy
...
```

#### Example

Has subject term/sao/Monster (or a narrower term, e.g., Drakar, Gorgoner).
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?matches-instanceOf.subject.@id=https://id.kb.se/term/sao/Monster'
...
```

#### Example

Has subject term/sao/Monster (or a narrower term, e.g., Drakar, Gorgoner), _and_ term/sao/Magi (or a narrower term,
e.g., Amuletter, Häxeri).
```
$ curl -XGET -H "Accept: application/ld+json" -G \
    'https://libris-qa.kb.se/find.jsonld' \
    -d and-matches-instanceOf.subject.@id=https://id.kb.se/term/sao/Monster \
    -d and-matches-instanceOf.subject.@id=https://id.kb.se/term/sao/Magi
...
```

### `/_remotesearch` - Search external databases - Requires authentication

This endpoint allows you to query external databases.

#### Parameters

* `q` - Search query
* `start` - Search result offset, used for pagination. Default is 0 (no
  offset).
* `n` - Max number of hits to include in result, used for pagination. Default
  is 10.
* `databases` - A comma separated list of databases to search. Used alone it
  will list available databases.

#### Examples

List available databases:

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

Search two external databases for the phrase "tove":

```
$ curl -XGET 'https://libris-qa.kb.se/_remotesearch?q=tove&databases=BIBSYS,DANBIB'
{"totalResults":{"BIBSYS":7336,"DANBIB":11991},"items":[...]}
```


### `/_convert` - Preview MARC21 conversion

This endpoint allows you to preview what a JSON-LD document would look like
converted to MARC21.

#### Example

```
$ curl -XGET -H "Content-Type: application/ld+json" \
    -H "Accept: application/x-marc-json" \
    -d@my_record.jsonld https://libris-qa.kb.se/_convert
...
```


### `/_findhold` - Find holding records for a bibliographic record

This endpoint will list the holding records that the specified library has for
the specified bibliographic record.

#### Parameters

* `id` - Bibliographic record ID (e.g. https://libris-qa.kb.se/s93ns5h436dxqsh or http://libris.kb.se/bib/1234)
* `library` - Library ID (e.g. https://libris.kb.se/library/SEK)

#### Example

```
$ curl -XGET 'https://libris-qa.kb.se/_findhold?id=https://libris-qa.kb.se/s93ns5h436dxqsh&library=https://libris.kb.se/library/SEK'
["https://libris-qa.kb.se/48h9kp894jm8kzz"]
$ curl -XGET 'https://libris-qa.kb.se/_findhold?id=http://libris.kb.se/bib/1234&library=https://libris.kb.se/library/SEK'
["https://libris-qa.kb.se/48h9kp894jm8kzz"]
```

### `/_dependencies` - List record dependencies

#### Parameters

* `id` - Bibliographic record ID (e.g. http://libris.kb.se/bib/1234)
* `relation` - Type of relation (this parameter is optional and may be omitted)
* `reverse` - Boolean to indicate reverse relation (defaults to false)

#### Example

```
$ curl -XGET 'https://libris-qa.kb.se/_dependencies?id=http://libris.kb.se/bib/1234&relation=language'
["https://libris-qa.kb.se/zfpl8t8310mn9s2m"]
```


### `/_compilemarc` - Download MARC21 bibliographic record with holding and authority information

This endpoint allows you to download a complete bibliographic record with holding information in MARC21. If the compiled record is to contain any holding information, there must be an export profile registered for that library ID. Contact support if you need to register such a profile.

#### Parameters

* `id` - Bibliographic record ID (e.g. http://libris.kb.se/bib/1234)
* `library` - Library ID (e.g. https://libris.kb.se/library/SEK)

#### Example

```
$ curl -XGET 'https://libris-qa.kb.se/_compilemarc?id=http://libris.kb.se/bib/1234&library=https://libris.kb.se/library/SEK'
01040cam a22002897  45000010005000000030008000050050017000130080041000300200015000710350015000860400008001010840013001090840014001220840016001360840017001520840018001690840017001872450047002042640038002513000021002897000029003107720121003398870107004608410051005678520020006188870112006381234SE-LIBR20180126135547.0011211s1971    xxu|||||||||||000 ||eng|   a0824711165  99900990307  aLi*  aUbb2sab  aUcef2sab  aUbb2kssb/5  aUcef2kssb/5  aUe.052kssb/5  aPpdc2kssb/500aWater and water pollution handbooknVol. 2 1aNew York :bMarcel Dekker ,c1971
 aS. 451-800bill.1 aCiaccio, Leonard L.4edt0 dNew York : Marcel Dekker, cop. 1971-1973w99000064967nn92ced. by L.L. CiacciotWater and water pollution handbook  a{"@id":"s93ns5h436dxqsh","modified":"2018-01-26T13:55:47.68+01:00","checksum":"12499440084"}2librisxl  5SEKax  ab150526||    |   1001||und|901128e4  5SEKbSEKhTEST2  5SEKa{"@id":"48h9kp894jm8kzz","modified":"2018-01-27T11:29:46.804+01:00","checksum":"-426929336"}2librisxl
```
