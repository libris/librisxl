package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*

@Log
class JsonLDTurtleConverter extends BasicRDFFormatConverter {

    String requiredContentType = "application/ld+json"
    def context
    def base

    JsonLDTurtleConverter(String contextPath, String base=null) {
        def loader = getClass().classLoader
        def contextSrc = loader.getResourceAsStream(contextPath).withStream {
            mapper.readValue(it, Map)
        }
        context = JsonLdToTurtle.parseContext(contextSrc)
        this.base = base
    }

    Map<String, RDFDescription> doConvert(Document doc) {
        Map<String, RDFDescription> docs = new HashMap<String, RDFDescription>()
        def source = mapper.readValue(doc.data, Map)
        def bytes = JsonLdToTurtle.toTurtle(context, source, base).toByteArray()
        docs.put(doc.identifier, new RDFDescription(identifier: doc.identifier, data: bytes, contentType: "text/turtle"))
        return docs
    }

}
