package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

@Log
class Location {

    Document document

    Location() {}

    Location(Document doc) {
        this.document = doc
    }

    URI getDescribedBy() { return null}
    URI found() { return null}

}
