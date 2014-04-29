package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.BasicIndexFormatConverter

import static se.kb.libris.conch.Tools.*

class DefsIndexFormatConverter extends BasicIndexFormatConverter implements IndexFormatConverter {
    String requiredContentType = "application/ld+json"

    List<Document> doConvert(Document doc) {
        def docmap = doc.dataAsMap
        docmap.remove("@context")
        return [doc.withData(docmap)]
    }
}
