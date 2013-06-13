package se.kb.libris.whelks.basic

import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.Resource
import se.kb.libris.whelks.IndexDocument

abstract class BasicSplittingFormatConverter<T> extends BasicPlugin {

    final List<T> convertBulk(List<Resource> docs) {
        List<T> outdocs = new ArrayList<IndexDocument>()
        for (doc in docs) {
            outdocs.addAll(convert(doc))
        }
        return outdocs
    }

    final List<T> convert(Resource doc) {
        List<T> outdocs = new ArrayList<IndexDocument>()
        if (doc.contentType == requiredContentType) {
            outdocs.addAll(doConvert(doc))
        } else {
            outdocs.add(new IndexDocument(doc))
        }
        return outdocs
    }

    abstract List<T> doConvert(Resource doc)
}
