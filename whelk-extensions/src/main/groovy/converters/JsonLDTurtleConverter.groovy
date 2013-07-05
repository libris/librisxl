package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDTurtleConverter extends BasicRDFFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    def context = null

    JsonLDTurtleConverter() {
        def loader = getClass().classLoader
        def contextSrc = loader.getResourceAsStream("context.jsonld").withStream {
            mapper.readValue(it, Map)
        }
        context = JsonLdToTurtle.parseContext(contextSrc)
    }

    List<RDFDescription> doConvert(RDFDescription doc) {
        List<RDFDescription> docs = []
        def source = mapper.readValue(doc.data, Map)
        def bytes = JsonLdToTurtle.toTurtle(context, source).toByteArray()
        docs << new RDFDescription(identifier: doc.identifier, data: bytes, contentType: "text/turtle")
        return docs
    }

}
