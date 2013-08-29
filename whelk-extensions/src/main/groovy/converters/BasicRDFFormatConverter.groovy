package se.kb.libris.whelks.basic

import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.RDFDescription
import se.kb.libris.whelks.Document

abstract class BasicRDFFormatConverter extends BasicPlugin implements RDFFormatConverter {

    final List<RDFDescription> convert(Document doc) {
        List<RDFDescription> outdocs = new ArrayList<RDFDescription>()
        if (doc.contentType == requiredContentType) {
            outdocs.addAll(doConvert(doc))
        }
        return outdocs
    }

    abstract List<RDFDescription> doConvert(Document doc)
}
