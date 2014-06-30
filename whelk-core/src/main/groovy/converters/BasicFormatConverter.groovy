package se.kb.libris.whelks.basic

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.plugin.*

@Log
abstract class BasicFormatConverter extends BasicPlugin implements FormatConverter {

    final List<Document> convertBulk(List<Document> docs) {
        def outdocs = []
        for (doc in docs) {
            outdocs.add(convert(doc))
        }
        return outdocs
    }

    final Document convert(final Document doc) {
        assert doc
        log.debug("Document is ${doc?.contentType} - required is $requiredContentType")
        Document newdocument = doc
        if (doc.contentType == requiredContentType) {
            long originalDocumentTimestamp = doc.timestamp
            log.debug("Running converter.")
            return doConvert(doc).withTimestamp(originalDocumentTimestamp)
        }
        return doc
    }


    abstract Document doConvert(final Document doc)
}
