package se.kb.libris.conch.data

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.SearchResult
import se.kb.libris.whelks.basic.BasicDocument

class WhelkDocument extends BasicDocument {
    URI identifier
    Document withIdentifier(URI u) {
        this.identifier = u
        return this
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
