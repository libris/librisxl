package whelk.plugin.libris

import groovy.util.logging.Slf4j as Log

import whelk.*
import whelk.plugin.*

@Log
class CleanupIndexFormatConverter extends BasicFilter {

    Document doFilter(Document doc) {
        def docmap = doc.dataAsMap
        def ct = docmap.remove("@context")
        def broken = docmap.remove("broken")
        if (ct || broken) {
            // Only update document if there actually was a change
            doc.withData(docmap)
        }
        return doc
    }

    Map doFilter(Map docmap) {
        def ct = docmap.remove("@context")
        def broken = docmap.remove("broken")
        return docmap
    }

    boolean valid(Document doc) {
        if (doc && doc.isJson() && doc.contentType == "application/ld+json") {
            return true
        }
        return false
    }
}
