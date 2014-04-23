package se.kb.libris.whelks.basic

import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.Document

abstract class BasicIndexFormatConverter extends BasicPlugin implements IndexFormatConverter {

    final List<Document> convert(Document doc) {
        List<Document> outdocs = new ArrayList<Document>()
        if (doc.contentType == requiredContentType) {
            outdocs.addAll(doConvert(doc))
        }
        return outdocs
    }

    abstract List<Document> doConvert(Document doc)
}
