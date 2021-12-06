package whelk.converter

import groovy.util.logging.Log4j2 as Log
import whelk.JsonLd
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.util.PropertyLoader

import static whelk.util.Jackson.mapper

@Log
class JsonLDTrigConverter implements FormatConverter {
    String resultContentType = "text/trig"
    String requiredContentType = "application/ld+json"
    def context
    def base
    
    JsonLDTrigConverter(String base = null, Whelk whelk = null) {
        if (whelk) {
            context = JsonLdToTurtle.parseContext(['@context': whelk.jsonld.context])
        } else {
            readContextFromDb()
        }
        this.base = base
    }

    private synchronized readContextFromDb() {
        if (context == null) {
            Properties props = PropertyLoader.loadProperties("secret")
            PostgreSQLComponent postgreSQLComponent = new PostgreSQLComponent(props.getProperty("sqlUrl"))
            context = JsonLdToTurtle.parseContext(mapper.readValue(postgreSQLComponent.getContext(), HashMap.class) )
        }
    }

    Map convert(Map source, String id) {
        def bytes = JsonLdToTurtle.toTrig(context, source, base, id).toByteArray()
        return [(JsonLd.NON_JSON_CONTENT_KEY) : (new String(bytes, "UTF-8"))]
    }
}
