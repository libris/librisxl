package whelk.converter

import groovy.util.logging.Log4j2 as Log
import whelk.JsonLd
import whelk.Whelk

import static java.nio.charset.StandardCharsets.UTF_8

@Log
class JsonLDTurtleConverter implements FormatConverter {

    String resultContentType = "text/turtle"
    String requiredContentType = "application/ld+json"
    def base

    JsonLDTurtleConverter(String base = null, Whelk whelk = null) {
        this.base = base
    }

    Map convert(Map source, String id) {
        return [(JsonLd.NON_JSON_CONTENT_KEY) : toTurtle(source, null, base)]
    }

    static String toTurtleData(source, Map context) {
        // Add skip prelude flag in trld.trig.SerializerState.serialize?
        return withoutPrefixes(toTurtle(source, context, null))
    }

    private static String toTurtle(source, Map context, base) {
        def bytes = JsonLdToTrigSerializer.toTurtle(context, source, base).toByteArray()
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
