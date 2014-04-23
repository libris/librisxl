package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.BasicIndexFormatConverter

import static se.kb.libris.conch.Tools.*

class DefsIndexFormatConverter extends BasicIndexFormatConverter implements IndexFormatConverter {
    String requiredContentType = "application/ld+json"

    List<Document> doConvert(Document doc) {
        def docmap = getDataAsMap(doc)
        def idxDoc = new Document(doc)
        docmap.remove("@context")
        return [idxDoc.withData(getMapAsString(docmap))]
    }
}
