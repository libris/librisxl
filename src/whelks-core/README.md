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

    curl -XPUT -H "Content-Type:application/json" "http://<some-whelk-url>/<desired-document-path>" --data-binary @file.json

