About the MARC Example Data
========================================================================

## Getting example data ##

A bib post:

    $ curl -s 'http://libris.kb.se/data/bib/7149593?format=text' -o bib/7149593.marc
    $ curl -s 'http://libris.kb.se/data/bib/7149593?format=ISO2709' > /tmp/7149593.ISO2709

An auth post:

    $ curl -s 'http://libris.kb.se/data/auth/191503?format=text' -o auth/191503.marc
    $ curl -s 'http://libris.kb.se/data/auth/191503?format=ISO2709' > /tmp/191503.ISO2709

## Converting to JSON structs ##

The librisxl backend system has tools for converting ISO2709 to MARC-in-JSON.

