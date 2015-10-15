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
    protected MarcConversion conversion
    final static ObjectMapper mapper = new ObjectMapper()


    @Override
    String getResultContentType() { "application/x-marc-json" }

    @Override
    String getRequiredContentType() { "application/ld+json" }

    JsonLD2MarcConverter() {
        def loader = getClass().classLoader

        loader.getResourceAsStream("ext/oldspace.json").withStream {
            uriMinter = new URIMinter(mapper.readValue(it, Map))
        }

        def config = loader.getResourceAsStream("ext/marcframe.json").withStream {
            mapper.readValue(it, Map)
        }
        def tokenMaps = [:]
        config.tokenMaps.each { key, sourceRef ->
            if (sourceRef instanceof String) {
                tokenMaps[key] = loader.getResourceAsStream("ext/" + sourceRef).withStream {
                    mapper.readValue(it, List).collectEntries { [it.code, it] }
                }
            } else {
                tokenMaps[key] = sourceRef
            }
        }
        conversion = new MarcConversion(config, uriMinter, tokenMaps)
    }

    Document convert(final Document doc) {
        def source = doc.data
        def result = conversion.revert(source)
        log.trace("Created frame: $result")
        return new Document(doc.identifier, result, doc.manifest).withContentType(getResultContentType())
    }
}
