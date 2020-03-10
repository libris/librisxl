# Libris XL 'REST' API

**NOTE:** This document is a work in progress. It can be useful for testing,
but the interface is likely to change.

## CRUD API

Libris XL uses JSON-LD as data format, and we provide an API to create, read,
update, and delete records. Read operations are available without authentication,
but all other requests require an access token. There are two different
permission levels for users: one for working with holding records and one for
cataloging (which also allows working with holding records). User permissions are
connected to sigels, and a user may only work with holding records belonging to a
sigel that the user has permissions for. More information about authentication
can be found in the Authentication section in this document.

We have version handling of documents and for this we keep track of which sigel
is responsible for the change. Because of this, we require the header
`XL-Active-Sigel` to be set to the authenticated user's selected sigel.

An example of how to work with the API can be found in our [integration
tests](https://github.com/libris/lxl_api_tests).

### Reading a record

You read a record by sending a `GET` request to the record's URI (e.g.
`https://libris-qa.kb.se/fnrbrgl`) with the `Accept` header set to e.g.
`application/ld+json`. The default content type is `text/html`, which would
give you an HTML rendering of the record and not just the underlying data.

The response also contains an `ETag` header, which you must use (as a
If-Match header) if you are going to update the record.

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

### Creating a record - Requires authentication

You create a new record by sending a `POST` request to the API root (`/`) with at least the
`Content-Type`, `Authorization`, and `XL-Active-Sigel` headers set.

There are some checks in place, e.g. in order to prevent creation of duplicate
holding records, and to these requests the API responds with a `400 Bad Request`
with an error message explaining the issue.

A successful creation will get a `201 Created` response with a `Location`
header containing the URI of the new record.

```
$ curl -XPOST -H "Content-Type: application/ld+json" \
    -H "Authorization: Bearer xxxx" -d@my_record.jsonld \
    https://libris-qa.kb.se/data/
...
```


### Updating a record - Requires authentication

You update an existing record by sending a `PUT` request to the record URI (e.g.
`https://libris-qa.kb.se/fnrbrgl`) with at least the `Content-Type`,
`Authorization`, `If-Match`, and `XL-Active-Sigel` headers set.

To prevent accidentally overwriting a record modified by someone else, you need
to set the `If-Match` header to the value of the `ETag` header you got back
when you read the record in question. If the record had been modified by someone
else in, you will get a `409 Conflict` back.

You are not allowed to change the ID of a record, for that you must instead first
delete the original record and then create the replacement.

A successful update will get a `204 No Content` response, and invalid requests
will get a `400 Bad Request` response, with an error message.


#### Example

```
$ curl -XPUT -H "Content-Type: application/ld+json" \
    -H "Authorization: Bearer xxxx" -d@my_updated_record.jsonld \
    https://libris-qa.kb.se/fnrbrgl
...
```


### Deleting a record - Requires authentication

You delete a record by sending a `DELETE` request to the record URI (e.g.
`https://libris-qa.kb.se/fnrbrgl`) with the `Authorization` and
`XL-Active-Sigel` headers set.

You can only delete records that are not linked to by other records. That is, if
you want to delete a bibliographic record, you can only do so if there are no
holding records related to it. If there are holding records related to it, the API
will respond with a `403 Forbidden`.

A successful deletion will get a `204 No Content` response. Any subsequent
requests to the record's URI will get a `410 Gone` response.


#### Example

```
$ curl -XDELETE -H "Authorization: Bearer xxxx" \
    https://libris.kb.se/fnrbrgl
...
```


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

Records can be filtered on field values. Multiple fields means `AND`. Multiple values
for the same field means `OR`. Specifying multiple values for the same field can be done
by repeating the parameter or by giving a comma-separated list as value.

* `<field>` - The record has exactly this value for `field`.  
* `min-<field>` - Greater or equal to.
* `minEx-<field>` - Greater than (exclusive minimum).
* `max-<field>` - Less or equal to.
* `maxEx-<field>` - Less than.
* `matches-<field>` - Value is matching (see date-search below).

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
    'https://libris-qa.kb.se/find.jsonld?meta.descriptionCreator=https://libris.kb.se/library/S&matches-meta.created=2018-W08,2018W10&_limit=2'
...
```

#### Example

Containing 'Aniara' and held by sigel APP1.
```
$ curl -XGET -H "Accept: application/ld+json" \
    'https://libris-qa.kb.se/find.jsonld?q=Aniara&@reverse.itemOf.heldBy.@id=https://libris.kb.se/library/APP1'
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

This endpoint allows you to download a complete bibliographic record with holding
information in MARC21.

#### Parameters

* `id` - Bibliographic record ID (e.g. http://libris.kb.se/bib/1234)
* `library` - Library ID (e.g. https://libris.kb.se/library/SEK)

#### Example

```
$ curl -XGET 'https://libris-qa.kb.se/_compilemarc?id=http://libris.kb.se/bib/1234&library=https://libris.kb.se/library/SEK'
01040cam a22002897  45000010005000000030008000050050017000130080041000300200015000710350015000860400008001010840013001090840014001220840016001360840017001520840018001690840017001872450047002042640038002513000021002897000029003107720121003398870107004608410051005678520020006188870112006381234SE-LIBR20180126135547.0011211s1971    xxu|||||||||||000 ||eng|   a0824711165  99900990307  aLi*  aUbb2sab  aUcef2sab  aUbb2kssb/5  aUcef2kssb/5  aUe.052kssb/5  aPpdc2kssb/500aWater and water pollution handbooknVol. 2 1aNew York :bMarcel Dekker ,c1971
 aS. 451-800bill.1 aCiaccio, Leonard L.4edt0 dNew York : Marcel Dekker, cop. 1971-1973w99000064967nn92ced. by L.L. CiacciotWater and water pollution handbook  a{"@id":"s93ns5h436dxqsh","modified":"2018-01-26T13:55:47.68+01:00","checksum":"12499440084"}2librisxl  5SEKax  ab150526||    |   1001||und|901128e4  5SEKbSEKhTEST2  5SEKa{"@id":"48h9kp894jm8kzz","modified":"2018-01-27T11:29:46.804+01:00","checksum":"-426929336"}2librisxl
```

## Authentication

The API uses [Libris Login](https://login.libris.kb.se) as OAuth2 provider. All
users need a personal account and this means that if you will need OAuth2
client credentials if you want to authenticate users.

If the authentication is successful, you will get back a bearer token, a
refresh token, and a list of permissions for the user. This list can (and
probably should) be used to allow the user to select which sigel they want to
work as. This information is required for creating, updating, and deleting
records (see the CRUD API section in this document for more details).

Once the user is authenticated, you include the bearer token in the API
requests by setting the `Authentication` header to `Bearer: <the bearer
token>`.

If the API cannot validate the token, it will respond with a `401 Unauthorized`
with a message saying either that the token is invalid, that it has expired, or
that it is missing.
