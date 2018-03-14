# Libris XL 'REST' API

**NOTE:** This document is a work in progress. It can be useful for testing,
but the interface is likely to change before the system is in production.

## CRUD API

Libris XL uses JSON-LD as data format, and we provide an API to create, read,
update, and delete posts. Read operations are available without authentication,
but all other requests require an access token. There are two different
permission levels for users: one for working with holding posts and one for
cataloging (which also allows working with holding posts). User permissions are
connected to sigels, and a user may only work with holding posts belonging to a
sigel that the user has permissions for. More information about authentication
can be found in the Authentication section in this document.

We have version handling of documents and for this we keep track of which sigel
is responsible for the change. Because of this, we require the header
`XL-Active-Sigel` to be set to the authenticated user's selected sigel.

An example of how to work with the API can be found in our [integration
tests](https://github.com/libris/lxl_api_tests).

### Reading a post

You read a post by sending a `GET` request to the post's URI (e.g.
`https://libris-qa.kb.se/fnrbrgl`) with the `Accept` header set to e.g.
`application/ld+json`. The default content type is `text/html`, which would
give you an HTML rendering of the post and not just the underlying data.

The response also contains an `ETag` header, which you must use (as a
If-Match header) if you are going to update the post.

#### Example

```
$ curl -XGET -H "Accept: application/ld+json" https://libris-qa.kb.se/s93ns5h436dxqsh
{
  "@context": "/context.jsonld",
  "@graph": [
    ...
  ]
}
```


### Creating a post - Requires authentication

You create a new post by sending a `POST` request to the API root (`/`) with at least the
`Content-Type`, `Authorization`, and `XL-Active-Sigel` headers set.

There are some checks in place, e.g. in order to prevent creation of duplicate
holding posts, and to these requests the API responds with a `400 Bad Request`
with an error message explaining the issue.

A successful creation will get a `201 Created` response with a `Location`
header containing the URI of the new post.

```
$ curl -XPOST -H "Content-Type: application/ld+json" \
    -H "Authorization: Bearer xxxx" -d@my_post.jsonld \
    https://libris-qa.kb.se/
...
```


### Updating a post - Requires authentication

You update an existing post by sending a `PUT` request to the post URI (e.g.
`https://libris-qa.kb.se/fnrbrgl`) with at least the `Content-Type`,
`Authorization`, `If-Match`, and `XL-Active-Sigel` headers set.

To prevent accidentally overwriting a post modified by someone else, you need
to set the `If-Match` header to the value of the `ETag` header you got back
when you read the post in question. If the post had been modified by someone
else in, you will get a `409 Conflict` back.

You are not allowed to change the ID of a post, for that you must instead first
delete the original post and then create the replacement.

A successful update will get a `204 No Content` response, and invalid requests
will get a `400 Bad Request` response, with an error message.


#### Example

```
$ curl -XPUT -H "Content-Type: application/ld+json" \
    -H "Authorization: Bearer xxxx" -d@my_updated_post.jsonld \
    https://libris-qa.kb.se/fnrbrgl
...
```


### Deleting a post - Requires authentication

You delete a post by sending a `DELETE` request to the post URI (e.g.
`https://libris-qa.kb.se/fnrbrgl`) with the `Authorization` and
`XL-Active-Sigel` headers set.

You can only delete posts that are not linked to by other posts. That is, if
you want to delete a bibliographic post, you can only do so if there are no
holding posts related to it. If there are holding posts related to it, the API
will respond with a `403 Forbidden`.

A successful deletion will get a `204 No Content` response. Any subsequent
requests to the post's URI will get a `410 Gone` response.


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

### Parameters

* `q` - Search query
* `_limit` - Max number of hits to include in result, used for pagination.
  Default is 200.
* `_offset` - Number of hits to skip in the result, used for pagination.
  Default is 0.

### Example

```
$ curl -XGET -H "Accept: application/ld+json" \
    https://libris-qa.kb.se/find\?q\=tove%20\(jansson\|lindgren\)\&_limit=2
...
```


### `/_remotesearch` - Search external databases

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
$ curl -XGET 'https://libris-qa.kb.se/_remotesearch?databases=true'
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
    -d@my_post.jsonld https://libris-qa.kb.se/_convert
...
```


### `/_findhold` - Find holding posts for a bibliographic post

This endpoint will list the holding posts that the specified library has for
the specified bibliographic post.

#### Parameters

* `id` - Bibliographic post ID (e.g. http://libris.kb.se/bib/1234)
* `library` - Library ID (e.g. https://libris.kb.se/library/SEK)

#### Example

```
$ curl -XGET 'https://libris-qa.kb.se/_findhold?id=http://libris.kb.se/bib/1234&library=https://libris.kb.se/library/SEK'
["https://libris-qa.kb.se/48h9kp894jm8kzz"]
```

### `/_merge` - Merge two posts - Requires authentication

This endpoint allows you to automatically merge two bibliographic posts. This
is useful when you have a duplicate post. The resulting merged post will
generally contain information from both of the original posts, but only
the original post identified as `id1` is guaranteed to have all it's
information present in the resulting merged post. Information from the
other post will be added where possible (where not in conflict with the
first post).

Calling this with two unrelated posts is never a good idea.

The endpoint allows GET requests to preview the merge, while POST requests
stores the result in the database.

POST requests requires a valid access token, which must be set in the
`Authorization` header.

#### Parameters

* `id1` - First bibliographic post (e.g. http://libris.kb.se/bib/1234)
* `id2` - Second bibliographic post (e.g. http://libris.kb.se/bib/7149593)
* `promote_id2` - Boolean to indicate that `id2` should be used instead of
  `id1` for the resulting post (defaults to false)

#### Examples

Preview a merge of two unrelated posts:

```
$ curl -XGET 'https://libris-qa.kb.se/_merge?id1=http://libris.kb.se/bib/1234&id2=http://libris.kb.se/bib/7149593'
...
```

**NOTE:** The above example is only useful to see how the merge works. Never
ever try to merge two unrelated posts like this.

### `/_dependencies` - List post dependencies

#### Parameters

* `id` - Bibliographic post ID (e.g. http://libris.kb.se/bib/1234)
* `relation` - Type of relation (this parameter is optional and may be omitted)
* `reverse` - Boolean to indicate reverse relation (defaults to false)

#### Example

```
$ curl -XGET 'https://libris-qa.kb.se/_dependencies?id=http://libris.kb.se/bib/1234&relation=language'
["https://libris-qa.kb.se/zfpl8t8310mn9s2m"]
```


### `/_compilemarc` - Download MARC21 bibliographic post with holding and authority information

This endpoint allows you to download a complete bibliographic post with holding
information in MARC21.

#### Parameters

* `id` - Bibliographic post ID (e.g. http://libris.kb.se/bib/1234)
* `library` - Library ID (e.g. https://libris.kb.se/library/SEK)

### Example

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
posts (see the CRUD API section in this document for more details).

Once the user is authenticated, you include the bearer token in the API
requests by setting the `Authentication` header to `Bearer: <the bearer
token>`.

If the API cannot validate the token, it will respond with a `401 Unauthorized`
with a message saying either that the token is invalid, that it has expired, or
that it is missing.
