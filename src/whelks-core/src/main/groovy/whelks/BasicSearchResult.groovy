package se.kb.libris.whelks.basic

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.SearchResult

import groovy.json.*

class BasicSearchResult implements SearchResult {

    Iterable hits
    Iterable facets

    BasicSearchResult() {
        this.hits = new ArrayList<Document>()
    }

    void addHit(Document d) {
        this.hits.add(d)
    }

    void addHit(Document d, Map<String, String[]> highlightedFields) {
        def doc = new HighlightedDocument(d, highlightedFields)
        this.hits.add(doc)
    }

    int getNumberOfHits() {
        return this.hits.size()
    }

    def String toJson() {
        def jsonString = new StringBuilder()
        jsonString << "["
        hits.eachWithIndex() { it, i ->
            if (i > 0) { jsonString << "," }
            jsonString << it.dataAsString
        }
        jsonString << "]"
        return jsonString.toString()
    }
}

class HighlightedDocument extends BasicDocument {
    Map<String, String[]> matches = new TreeMap<String, String[]>()

    HighlightedDocument(Document d, Map<String, String[]> match) {
        withData(d.getData())
        this.matches = match
    }

    @Override
    String getDataAsString() {
        def slurper = new JsonSlurper()
        def json = slurper.parseText(super.getDataAsString())
        json.highlight = matches
        def builder = new JsonBuilder(json)
        return builder.toString()
    } 
}
