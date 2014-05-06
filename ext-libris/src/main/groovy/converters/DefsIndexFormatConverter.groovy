package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.BasicFormatConverter

@Log
class DefsIndexFormatConverter extends BasicFormatConverter {
    String requiredContentType = "application/ld+json"
    String resultContentType = "application/ld+json"

    Document doConvert(Document doc) {
        def docmap = doc.dataAsMap
        docmap.remove("@context")
        return doc.withData(docmap)
    }
}
