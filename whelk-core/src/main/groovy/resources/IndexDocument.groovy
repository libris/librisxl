package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.component.*

import org.codehaus.jackson.*
import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

@Log
class IndexDocument extends Document {

    Map<String, String[]> matches = new TreeMap<String, String[]>()
    String type
    String origin

    ObjectMapper mapper = new ElasticJsonMapper()

    IndexDocument() {
        this.contentType = "application/json"
    }
    IndexDocument(Document d) {
        this.identifier = d.identifier
        this.data = d.data
        this.entry = d.entry
        this.meta = d.meta

    }
    IndexDocument(IndexDocument d) {
        this.identifier = d.identifier
        this.data = d.data
        this.entry = d.entry
        this.meta = d.meta
    }
    IndexDocument(IndexDocument d, Map<String, String[]> match) {
        this.data = d.getData()
        withIdentifier(d.identifier).withContentType(d.contentType)
        this.matches = match
        this.entry = d.entry
        this.meta = d.meta
    }

    @Override
    String getDataAsString() {
        def json = this.getDataAsMap()
        return mapper.writeValueAsString(json)
    }

    Map getDataAsMap() {
        def json = super.getDataAsMap() //mapper.readValue(super.getDataAsString(), Map)
        json.highlight = matches
        if (origin) {
            json["extractedFrom"] = ["@id":origin]
        }
        return json
    }

    @Override
    byte[] getData() {
        return getDataAsString().getBytes("UTF-8")
    }

    IndexDocument withType(String t) {
        this.type = t
        return this
    }

    @Override
    IndexDocument withData(byte[] d) {
        this.data = d
        return this
    }

    IndexDocument withOrigin(String identifier) {
        this.origin = identifier
        return this
    }
}
