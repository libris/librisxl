package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDTurtleConverter extends BasicRDFFormatConverter {
    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()


    List<RDFDescription> doConvert(RDFDescription doc) {
        List<RDFDescription> docs = []
        return docs
    }
}
