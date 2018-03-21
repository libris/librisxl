package whelk

import groovy.util.logging.Log4j2 as Log

@Log
class Location {

    String id
    Document document
    URI uri

    int responseCode = 200

    Location() {}

    Location(Document doc) {
        this.document = doc
        this.id = doc.getShortId()
    }

    Location withResponseCode(int code) {
        responseCode = code
        return this
    }

    Location withId(String id) {
        this.id = id
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
