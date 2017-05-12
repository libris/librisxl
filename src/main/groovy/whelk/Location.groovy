package whelk

import groovy.util.logging.Slf4j as Log
import whelk.util.URIWrapper

@Log
class Location {

    String id
    Document document
    URIWrapper uri

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
        return withURI(new URIWrapper(location))
    }

    Location withURI(URIWrapper location) {
        uri = location
        return this
    }
}
