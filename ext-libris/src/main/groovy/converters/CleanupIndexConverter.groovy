package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.BasicFormatConverter

@Log
class CleanupIndexFormatConverter extends BasicFormatConverter {
    String requiredContentType = "application/ld+json"
    String resultContentType = "application/ld+json"

    Document doConvert(Document doc) {
        def docmap = doc.dataAsMap
        def ct = docmap.remove("@context")
        def unknw = docmap.remove("unknown")
        if (ct || unknw) {
            // Only update document if there actually was a context there to begin with
            doc.withData(docmap)
        }
        return doc
    }
}
