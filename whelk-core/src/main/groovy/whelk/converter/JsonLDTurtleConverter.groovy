package whelk.plugin

import groovy.util.logging.Slf4j as Log

import whelk.*

@Log
class JsonLDTurtleConverter extends BasicFormatConverter {

    String resultContentType = "text/turtle"
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

    Document doConvert(Document doc) {
        def source = mapper.readValue(doc.data, Map)
        def bytes = JsonLdToTurtle.toTurtle(context, source, base).toByteArray()
        return StandardWhelk.createDocument(resultContentType).withData(bytes).withIdentifier(doc.identifier)
    }

}
