package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.component.*

@Log
class IndexDocument extends AbstractDocument {

    Map<String, String[]> matches = new TreeMap<String, String[]>()

    IndexDocument() {}

    IndexDocument(IndexDocument d, Map<String, String[]> match) {
        withData(d.getData()).withIdentifier(d.identifier).withContentType(d.contentType)
        this.matches = match
    }

    String getDataAsString() {
        def mapper = new ElasticJsonMapper()
        def json = mapper.readValue(super.getDataAsString(), Map)
        json.highlight = matches
        return mapper.writeValueAsString(json)
    }
}
