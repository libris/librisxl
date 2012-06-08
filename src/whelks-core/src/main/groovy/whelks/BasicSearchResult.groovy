package se.kb.libris.whelks.basic

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.SearchResult

class BasicSearchResult implements SearchResult {

    Iterable hits
    Iterable facets

    BasicSearchResult() {
        this.hits = new ArrayList<Document>()
    }

    void addHit(Document d) {
        this.hits.add(d)
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
