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

To run the converter "offline", not connected to the actual whelk services
(e.g. for testing purposes), you must have a built copy of `definitions`
side-by-side with this repository. Otherwise, you can run it against a local
whelk with the correct local secret properties.

Convert one record from MARC to RDF:
```sh
$ ../gradlew runMarcFrame -Dargs="convert <path-to-some>.marcjson" 2>/dev/null | grep '^{'
```

Revert one (convert from RDF to MARC):
```sh
$ ../gradlew runMarcFrame -Dargs="revert <path-to-some>.jsonld" 2>/dev/null | grep '^{'
```
