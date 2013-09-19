package se.kb.libris.whelks.basic

import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.IndexDocument
import se.kb.libris.whelks.Document

abstract class BasicIndexFormatConverter extends BasicPlugin implements IndexFormatConverter {

    final List<IndexDocument> convert(Document doc) {
        List<IndexDocument> outdocs = new ArrayList<IndexDocument>()
        if (doc.contentType == requiredContentType) {
            outdocs.addAll(doConvert(doc))
        }
        return outdocs
    }

    abstract List<IndexDocument> doConvert(Document doc)
}
