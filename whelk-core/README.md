# Whelks Core README

## Dependencies

1. Install gradle from <http://gradle.org/> (or use a package manager, e.g.: brew install gradle).
2. Install elasticsearch from <http://elasticsearch.org/> (or use a package manager, e.g.: brew install elasticsearch).

## Configure elasticsearch

    $ cp src/main/resources/whelks-core.properties.in src/main/resources/whelks-core.properties
    $ vim src/main/resources/whelks-core.properties # ... (ask for directions)

## Run import

Warning! Don't do this against klustret unless you know what klustret means.

The import task uses this argument syntax:

    <bib|auth> [bib|auth] [since (in milliseconds/negative days/datetime)]

Example - import data to bib from resource bib since yesterday:

    $ gradle importData -Dargs='bib bib -1'

## Start the whelk

    $ gradle jettyRun

.. Running at <http://localhost:8080/>

## Adding data to storage:

    $ curl -XPUT -H "Content-Type:application/json" "http://<some-whelk-url>/<desired-document-path>" --data-binary @file.json

## Starting a local whelk with mock data

    $ gradle whelkOperation -Dargs='import bib file:src/main/resources/mock_whelks.json bib 1975-01-01T00:00:00Z 100' -Delastic.host='localhost' -Dfile.encoding='utf-8'

## Running data conversion on a single test document

    $ gradle convertMarc2JsonLD -Dargs="/bib/7149593 src/test/resources/marc2jsonld/in/bib/7149593.json build/"
    $ gradle convertJsonLD2Marc -Dargs="build/7149593.json /tmp/"

