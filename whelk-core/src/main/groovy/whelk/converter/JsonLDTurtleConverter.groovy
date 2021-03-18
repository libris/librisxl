package whelk.converter

import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.*
import whelk.component.PostgreSQLComponent
import whelk.util.PropertyLoader

@Log
class JsonLDTurtleConverter implements FormatConverter {

    String resultContentType = "text/turtle"
    String requiredContentType = "application/ld+json"
    def context
    def base
    def mapper = new ObjectMapper()

    JsonLDTurtleConverter(String base = null) {
        readContextFromDb()
        this.base = base
    }

    private synchronized readContextFromDb() {
        if (context == null) {
            Properties props = PropertyLoader.loadProperties("secret")
            PostgreSQLComponent postgreSQLComponent = new PostgreSQLComponent(props.getProperty("sqlUrl"))
            context = JsonLdToTurtle.parseContext( mapper.readValue(postgreSQLComponent.getContext(), HashMap.class) )
        }
    }

    Map convert(Map source, String id) {
        def bytes = JsonLdToTurtle.toTurtle(context, source, base).toByteArray()
        return [(JsonLd.NON_JSON_CONTENT_KEY) : (new String(bytes, "UTF-8"))]
    }
}
