package whelk.converter

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j as Log
import whelk.JsonLd
import whelk.Whelk

import static java.nio.charset.StandardCharsets.UTF_8

@Log
@CompileStatic
class JsonLDTurtleConverter implements FormatConverter {

    String resultContentType = "text/turtle"
    String requiredContentType = "application/ld+json"
    String base

    JsonLDTurtleConverter(String base = null, Whelk whelk = null) {
        this.base = base
    }

    Map convert(Map source, String id) {
        return [(JsonLd.NON_JSON_CONTENT_KEY) : toTurtle(source, null, base)]
    }

    static String toTurtleNoPrelude(source, Map context) {
        // Add skip prelude flag in trld.trig.SerializerState.serialize?
        return withoutPrefixes(toTurtle(source, context, null))
    }

    private static String toTurtle(Object source, Map context, String base) {
        byte[] bytes = JsonLdToTrigSerializer.toTurtle(context, source, base).toByteArray()
        return new String(bytes, UTF_8)
    }

    private static String withoutPrefixes(String ttl) {
        return ttl.readLines()
                .split { it.startsWith('prefix') }
                .get(1)
                .join('\n')
                .trim()
    }
}
