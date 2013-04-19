package se.kb.libris.whelks.http

import org.restlet.data.MediaType

class LibrisXLMediaType {

    static MediaType getMainMediaType(String ctype) {
        if (ctype =~ /application\/x-marc-json/ || ctype == "application/ld+json") {
            ctype = "application/json"
        }
        return new MediaType(ctype)
    }
}
