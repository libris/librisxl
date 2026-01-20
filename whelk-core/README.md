# Whelk Core

## LDDB

Each resource is stored within a Record (a unit of administrative metadata
along with one mainEntity). This is a JSON-LD representation of a named graph.

PostgreSQL with JSON support is used to store and index these, along with
versions thereof.

## Domain Knowledge

A JsonLd utility class is used to interpret vocabulary semantics. It is
configured with a JSON-LD context from which it picks the default
`@vocab`ulary, whose RDFS and OWL semantics is used to enable some basic logic
reasoning within the Whelk services (primarily for the search and data view
layers).

## Embellished Search Index

Embellished data is a framed representation of a concise bounded graph around
the mainEntity. The relevant data to pick is defined using FRESNEL lenses,
comprised by three levels: full, card and chips (plus tokens for short linked
parts within chips).

These embellished forms are stored in Elasticsearch, to enable text searches on
linked entities and faceting/aggregates upon linked information.

## Embellished Views

The embellished data can also be used for generating various views for
presentation and convenient processing containing useful snippets of
descriptions of related resources.

One such view is the framed, embellished view sent to the "revert" process,
which creates a MARC display record (since these consist of denormalized data).

## Legacy Library Data

The marcframe mappings specify specific paths from a certain MARC flavour to
RDF in the chosen vocabulary terms (MARC21 as used in Sweden is the default,
mapped to the KBV application vocabulary, which is based upon the BIBFRAME
(2.x) vocabulary.)

### Using The MarcFrameConverter

To convert one record from MARC to RDF:
```sh
$ ../gradlew runMarcFrame -Dargs="convert <path-to-some>.marcjson" 2>/dev/null | grep '^{'
```
To revert from RDF to MARC:
```sh
$ ../gradlew runMarcFrame -Dargs="revert <path-to-some>.jsonld" 2>/dev/null | grep '^{'
```

#### Running MarcFrame Locally Without Whelk

The converter requires metadata access to operate correctly. It can be used
with a local or remote whelk by providing a `xl.secret.properties` system
environment variable (via the `-D` cmdline flag to java or gradle).

To run the converter "offline", e.g. for testing purposes, you can use a built
copy of `definitions` side-by-side with this repository, *and* a cache of
important resource descriptions. These are referenced using
`xl.resourcecache.dir` and `xl.definitions.builddir`, respectively.
(See MarcFrameCli for details.)

To create the required cache files (here in `../../xl-resource-cache`), run:
```sh
$ ../gradlew runMarcFrame -Dxl.secret.properties=../SOME_ENV-secret.properties -Dargs="cachebytype ../../xl-resource-cache/typecache.json"
```
To run using this cache and the built syscore data in a local definitions clone:
```sh
$ ../gradlew runMarcFrame -Dxl.resourcecache.dir=../../xl-resource-cache -Dxl.definitions.builddir=../../definitions/build -Dargs="revert some.jsonld" | grep '^{' | python3 -m json.tool
```
A "debug" command is also defined to examine the typemappings used by the TypeCategoryNormalizer. It can be used with either a whelk instance or the above detailed cached files (as here):
```sh
$ ../gradlew runMarcFrame -Dxl.resourcecache.dir=../../xl-resource-cache -Dxl.definitions.builddir=../../definitions/build -Dargs="save-typemappings ../../xl-resource-cache/type-category-mappings.json"
```
