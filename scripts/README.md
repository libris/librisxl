About the MARC Example Data
========================================================================

## Getting example data ##

A bib post:

    $ curl -s 'http://libris.kb.se/data/bib/7149593?format=text' -o bib/7149593.marc
    $ curl -s 'http://libris.kb.se/data/bib/7149593?format=ISO2709' > /tmp/7149593.ISO2709
    $ curl -s 'http://libris.kb.se/data/bib/7149593?format=application/marcxml' > /tmp/7149593.ISO2709

An auth post:

    $ curl -s 'http://libris.kb.se/data/auth/191503?format=text' -o auth/191503.marc
    $ curl -s 'http://libris.kb.se/data/auth/191503?format=ISO2709' > /tmp/191503.ISO2709

## Converting to JSON structs ##

See convert-iso2709-to-json.sh

