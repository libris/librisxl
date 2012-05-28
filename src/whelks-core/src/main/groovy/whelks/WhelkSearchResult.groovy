package se.kb.libris.conch.data

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.SearchResult
import se.kb.libris.whelks.basic.BasicDocument

class WhelkSearchResult implements SearchResult {

    String result
    Iterable hits
    Iterable facets
    long numberOfHits

    WhelkSearchResult(String data, long hits) {
        this.result = data
        this.numberOfHits = hits
    }

    WhelkSearchResult(String data, int hits) {
        this.result = data
        this.numberOfHits = hits
    }

    int getNumberOfHits() {
        return (int)numberOfHits
    }
    
    void setNumberOfHits(int hits) {
        this.numberOfHits = hits
    }

    def String toString() {
        return result
    }

}
