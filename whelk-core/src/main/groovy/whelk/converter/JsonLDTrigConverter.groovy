package whelk.converter

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j as Log
import whelk.JsonLd
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.util.PropertyLoader

import static whelk.util.Jackson.mapper

@Log
@CompileStatic
class JsonLDTrigConverter implements FormatConverter {
    String resultContentType = "text/trig"
    String requiredContentType = "application/ld+json"
    String base

    JsonLDTrigConverter(String base = null, Whelk whelk = null) {
        this.base = base
    }

    Map convert(Map source, String id) {
        byte[] bytes = JsonLdToTrigSerializer.toTrig(null, source, base).toByteArray()
        return [(JsonLd.NON_JSON_CONTENT_KEY) : (new String(bytes, "UTF-8"))]
    }
}
