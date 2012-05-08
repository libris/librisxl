package se.kb.libris.conch.data

import se.kb.libris.whelks.SearchResult
import se.kb.libris.whelks.basic.BasicDocument

class MyDocument extends BasicDocument {
    URI identifier

    MyDocument(URI uri) {
        this.identifier = uri
    }
    MyDocument(String uri) {
        this.identifier = new URI(uri)
    }
}

class WhelkSearchResult implements SearchResult {

    String result

    WhelkSearchResult(String data) {
        this.result = data
    }

    def String toString() {
        return result
    }

}
