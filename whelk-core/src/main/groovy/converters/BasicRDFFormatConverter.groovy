package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.RDFDescription
import se.kb.libris.whelks.Document

abstract class BasicRDFFormatConverter extends BasicPlugin implements RDFFormatConverter {

    final Map<String, RDFDescription> convert(Document doc) {
        Map<String, RDFDescription> outdocs = new HashMap<String, RDFDescription>()
        if (doc.contentType == requiredContentType) {
            outdocs.putAll(doConvert(doc))
        }
        return outdocs
    }

    abstract Map<String, RDFDescription> doConvert(Document doc)
}
