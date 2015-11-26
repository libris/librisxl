package whelk.converter.marc

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.*
import whelk.converter.FormatConverter
import whelk.converter.URIMinter
import whelk.converter.marc.MarcConversion

@Log
class JsonLD2MarcConverter implements FormatConverter {

    URIMinter uriMinter
    protected MarcFrameConverter marcFrameConverter
    final static ObjectMapper mapper = new ObjectMapper()


    @Override
    String getResultContentType() { "application/x-marc-json" }

    @Override
    String getRequiredContentType() { "application/ld+json" }

    JsonLD2MarcConverter() {
        def marcFrameConverter = new MarcFrameConverter()
    }

    Document convert(final Document doc) {
        def source = doc.data
        def result = marcFrameConverter.runRevert(source)
        log.trace("Created frame: $result")
        return new Document(doc.identifier, result, doc.manifest).withContentType(getResultContentType())
    }
}
