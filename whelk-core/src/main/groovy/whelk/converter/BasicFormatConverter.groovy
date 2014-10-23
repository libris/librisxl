package whelk.plugin

import groovy.util.logging.Slf4j as Log

import whelk.Document
import whelk.plugin.*

@Log
abstract class BasicFormatConverter extends BasicPlugin implements FormatConverter {

    final List<Document> convertBulk(List<Document> docs) {
        def outdocs = []
        for (doc in docs) {
            outdocs.add(convert(doc))
        }
        return outdocs
    }

    Document transmogrify(Document doc) {
        return convert(doc)
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
