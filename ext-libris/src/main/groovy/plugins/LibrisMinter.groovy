package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*

@Log
class LibrisMinter extends BasicPlugin implements URIMinter {

    URI mint(Document doc, boolean remint = true) {
        if (!remint && doc.identifier) {
            return new URI(doc.identifier)
        }
        // This is the last resort.
        return new URI("/"+ UUID.randomUUID())
    }
}
