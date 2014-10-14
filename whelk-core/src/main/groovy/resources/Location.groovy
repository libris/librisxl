package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

@Log
class Location {

    Document document
    URI uri

    int responseCode = 200

    Location() {}

    Location(Document doc) {
        this.document = doc
    }

    Location withResponseCode(int code) {
        responseCode = code
        return this
    }

    Location withURI(String location) {
        return withURI(new URI(location))
    }

    Location withURI(URI location) {
        uri = location
        return this
    }
}
