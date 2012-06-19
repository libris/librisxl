package se.kb.libris.whelks.basic

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.SearchResult

import groovy.json.*

class BasicSearchResult implements SearchResult {

    Iterable hits
    Iterable facets

    long numberOfHits = 0

    BasicSearchResult(long nrHits) {
        this.numberOfHits = nrHits
        this.hits = new ArrayList<Document>()
    }

    void setNumberOfHits(int nrHits) {
        this.numberOfHits = nrHits
    }

    void addHit(Document d) {
        this.hits.add(d)
    }

    void addHit(Document d, Map<String, String[]> highlightedFields) {
        def doc = new HighlightedDocument(d, highlightedFields)
        this.hits.add(doc)
    }

    def String toJson() {
        def jsonString = new StringBuilder()
        jsonString << "{"
        jsonString << "\"hits\": " << numberOfHits << ","
        jsonString << "\"list\": ["
        hits.eachWithIndex() { it, i ->
            if (i > 0) { jsonString << "," }
            jsonString << "{\"identifier\": \"" << it.identifier << "\","
            jsonString << "\"data\":" << it.dataAsString << "}"
        }
        jsonString << "]"
        jsonString << "}"
        return jsonString.toString()
    }
}

class HighlightedDocument extends BasicDocument {
    Map<String, String[]> matches = new TreeMap<String, String[]>()

    HighlightedDocument(Document d, Map<String, String[]> match) {
        withData(d.getData()).withIdentifier(d.identifier).withContentType(d.contentType)
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
