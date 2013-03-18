package se.kb.libris.whelks.basic

import se.kb.libris.whelks.Document

abstract class BasicFormatConverter extends BasicPlugin {

    List<Document> convertBulk(List<Document> docs) {
        def outdocs = []
        for (doc in docs) {
            outdocs.addAll(convert(doc))
        }
        return outdocs
    }

    final List<Document> convert(Document doc) {
        def outdocs = []
        if (doc.contentType == requiredContentType && doc.format == requiredFormat) {
            outdocs.addAll(doConvert(doc))
        } else {
            outdocs.add(doc)
        }
        return outdocs
    }

    abstract List<Document> doConvert(Document doc)
}
