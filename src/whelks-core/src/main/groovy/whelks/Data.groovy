package se.kb.libris.conch.data

import se.kb.libris.whelks.basic.BasicDocument

class MyDocument extends BasicDocument {
    URI identifier

    MyDocument() {
        generate_identifier()
    }

    MyDocument(URI uri) {
        this.identifier = uri
    }
    MyDocument(String uri) {
        this.identifier = new URI(uri)
    }
}
