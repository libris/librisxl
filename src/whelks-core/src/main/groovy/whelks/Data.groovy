package se.kb.libris.conch.data

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.SearchResult
import se.kb.libris.whelks.basic.BasicDocument

class WhelkSearchResult implements SearchResult {

    String result
    Iterable hits
    Iterable facets
    int numberOfHits

    WhelkSearchResult(String data, int hits) {
        this.result = data
        this.numberOfHits = hits
    }

    def String toString() {
        return result
    }

}
