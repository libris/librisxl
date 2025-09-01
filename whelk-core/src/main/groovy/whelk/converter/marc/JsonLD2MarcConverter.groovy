package whelk.converter.marc

import groovy.util.logging.Log4j2 as Log
import whelk.converter.FormatConverter

@Log
class JsonLD2MarcConverter implements FormatConverter {

    protected MarcFrameConverter marcFrameConverter
    
    @Override
    String getResultContentType() { "application/x-marc-json" }

    @Override
    String getRequiredContentType() { "application/ld+json" }

    JsonLD2MarcConverter(MarcFrameConverter marcFrameConverter) {
        this.marcFrameConverter = marcFrameConverter
    }

    Map convert(Map data, String id) {
        def result = marcFrameConverter.runRevert(data)
        if (log.isTraceEnabled()) {
            log.trace("Created frame: $result")
        }
        return result
    }
}
