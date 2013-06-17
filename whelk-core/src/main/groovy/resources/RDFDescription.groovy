package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

@Log
class RDFDescription extends Resource {
    RDFDescription() {}
    RDFDescription(Document d) {
        this.identifier = d.identifier
        this.data = d.data
        this.contentType = d.contentType
    }
}

