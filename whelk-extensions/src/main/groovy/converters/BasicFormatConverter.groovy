package se.kb.libris.whelks.basic

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.plugin.*

abstract class BasicFormatConverter extends BasicPlugin implements FormatConverter {

    List<Document> convertBulk(List<Document> docs) {
        def outdocs = []
        for (doc in docs) {
            outdocs.add(convert(doc))
        }
        return outdocs
    }

    final Document convert(Document doc) {
        if (doc.contentType == requiredContentType) {
            doc = doConvert(doc)
        }
        return doc
    }

    abstract Document doConvert(Document doc)
}
