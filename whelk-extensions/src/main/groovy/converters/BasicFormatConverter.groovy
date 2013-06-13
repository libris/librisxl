package se.kb.libris.whelks.basic

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.plugin.*

abstract class BasicFormatConverter extends BasicPlugin {

    final List<Document> convertBulk(List<Document> docs) {
        def outdocs = []
        for (doc in docs) {
            outdocs.add(convert(doc))
        }
        return outdocs
    }

    final Document convert(Document doc) {
        log.debug("Document is ${doc.contentType} - required is $requiredContentType")
        if (doc.contentType == requiredContentType) {
            log.debug("Running converter.")
            doc = doConvert(doc)
        }
        return doc
    }

    abstract Document doConvert(Document doc)
}
