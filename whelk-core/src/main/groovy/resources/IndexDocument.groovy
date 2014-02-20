package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.component.*

import org.codehaus.jackson.*
import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

@Log
class IndexDocument extends Document {
/*
}

@Log
class OldIndexDocument extends Resource {
*/

    Map<String, String[]> matches = new TreeMap<String, String[]>()
    String type

    ObjectMapper mapper = new ElasticJsonMapper()

    IndexDocument() {}
    IndexDocument(Document d) {
        this.identifier = d.identifier
        this.data = d.data
        this.contentType = d.contentType
    }
    IndexDocument(IndexDocument d) {
        this.identifier = d.identifier
        this.data = d.data
        this.contentType = d.contentType
    }
    IndexDocument(IndexDocument d, Map<String, String[]> match) {
        this.data = d.getData()
        withIdentifier(d.identifier).withContentType(d.contentType)
        this.matches = match
    }

    @Override
    String getDataAsString() {
        def json = mapper.readValue(super.getDataAsString(), Map)
        json.highlight = matches
        return mapper.writeValueAsString(json)
    }

    Map getDataAsMap() {
        def json = mapper.readValue(super.getDataAsString(), Map)
        json.highlight = matches
        return json
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
}
