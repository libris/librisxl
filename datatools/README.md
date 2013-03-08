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

The librisxl backend system has tools for converting ISO2709 to MARC-in-JSON.

## Generate the marcmap

(Ask a co-worker for the config file sources.)

1. Get Swedish legacy config files and put them in a dir:

    $ CONFIG_SV=...

2. Get English legacy config files and put them in a dir:

    $ CONFIG_EN=...

3. Patch these files by hand (see parse_legacy_config.py for FIXME notes describing how).

4. Define (relative to this file):

    $ MARCMAP=../whelk-core/src/main/resources/marcmap.json

5. Run:

    $ python parse_legacy_config.py $CONFIG_EN/TagTable/Marc21 en $CONFIG_SV/TagTable/Marc21 sv > $MARCMAP

6. Download a FRBR CSV mapping:

     $ FRBR_CSV=...
     $ curl http://www.loc.gov/marc/marc-functional-analysis/source/FRBR_Web_Copy.txt -o $FRBR_CSV

7. Add frbr hints as entity annotations in the marcmap by running:

    $ python frbrize_marcmap.py $FRBR_CSV $MARCMAP > tmpout && mv tmpout $MARCMAP

