package se.kb.libris.whelks.basic

import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.IndexDocument

abstract class BasicIndexFormatConverter extends BasicPlugin implements IndexFormatConverter {

    final List<IndexDocument> convertBulk(List<IndexDocument> docs) {
        List<IndexDocument> outdocs = new ArrayList<IndexDocument>()
        for (doc in docs) {
            outdocs.addAll(convert(doc))
        }
        return outdocs
    }

    final List<IndexDocument> convert(IndexDocument doc) {
        List<IndexDocument> outdocs = new ArrayList<IndexDocument>()
        if (doc.contentType == requiredContentType) {
            outdocs.addAll(doConvert(doc))
        } else {
            outdocs.add(doc)
        }
        return outdocs
    }

    abstract List<IndexDocument> doConvert(IndexDocument doc)
}
