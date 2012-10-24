package se.kb.libris.whelks.basic

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.SearchResult

import org.codehaus.jackson.map.ObjectMapper

class BasicSearchResult implements SearchResult {

    Iterable hits
    Map facets
    ObjectMapper mapper

    long numberOfHits = 0

    BasicSearchResult(long nrHits) {
        this.numberOfHits = nrHits
        this.hits = new ArrayList<Document>()
        this.mapper = new ObjectMapper()
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
        if (facets) {
            jsonString << ",\"facets\":" << jsonifyFacets()
        }
        jsonString << "}"
        return jsonString.toString()
    }

    private jsonifyFacets() {
        return mapper.writeValueAsString(facets)
    }
}

