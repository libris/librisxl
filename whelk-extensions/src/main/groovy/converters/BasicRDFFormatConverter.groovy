package se.kb.libris.whelks.basic

import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.RDFDescription

abstract class BasicRDFFormatConverter extends BasicPlugin implements RDFFormatConverter {

    final List<RDFDescription> convertBulk(List<RDFDescription> docs) {
        List<RDFDescription> outdocs = new ArrayList<RDFDescription>()
        for (doc in docs) {
            outdocs.addAll(convert(doc))
        }
        return outdocs
    }

    final List<RDFDescription> convert(RDFDescription doc) {
        List<RDFDescription> outdocs = new ArrayList<RDFDescription>()
        if (doc.contentType == requiredContentType) {
            outdocs.addAll(doConvert(doc))
        }
        return outdocs
    }

    abstract List<RDFDescription> doConvert(RDFDescription doc)
}
