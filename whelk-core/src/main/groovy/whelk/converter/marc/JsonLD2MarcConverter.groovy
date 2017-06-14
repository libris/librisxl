package whelk.converter.marc

import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.*
import whelk.converter.FormatConverter

@Log
class JsonLD2MarcConverter implements FormatConverter {

    protected MarcFrameConverter marcFrameConverter
    final static ObjectMapper mapper = new ObjectMapper()


    @Override
    String getResultContentType() { "application/x-marc-json" }

    @Override
    String getRequiredContentType() { "application/ld+json" }

    JsonLD2MarcConverter() {
        marcFrameConverter = new MarcFrameConverter()
    }

    Map convert(Map data, String id) {
        def result = marcFrameConverter.runRevert(data)
        log.trace("Created frame: $result")
        return result
    }
}
