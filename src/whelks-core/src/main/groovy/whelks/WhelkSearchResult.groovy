package se.kb.libris.whelks.basic

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.SearchResult

class BasicSearchResult implements SearchResult {

    String result
    Iterable hits
    Iterable facets
    long numberOfHits = 0

    BasicSearchResult(String data, long hits) {
        this.result = data
        this.numberOfHits = hits
    }

    BasicSearchResult(String data, int hits) {
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
