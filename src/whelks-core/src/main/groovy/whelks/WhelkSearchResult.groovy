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

    def String toString() {
        return result
    }

}
