package whelk.plugin.libris

import groovy.util.logging.Slf4j as Log

import whelk.*
import whelk.plugin.*

@Log
class JsonLD2MarcConverter extends BasicFormatConverter {

    URIMinter uriMinter
    protected MarcConversion conversion

    @Override
    String getResultContentType() { "application/x-marc-json" }

    @Override
    String getRequiredContentType() { "application/ld+json" }

    JsonLD2MarcConverter() {
        def loader = getClass().classLoader

        loader.getResourceAsStream("ext/oldspace.json").withStream {
            uriMinter = new LibrisURIMinter(mapper.readValue(it, Map))
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

    @Override
    Document doConvert(final Document doc) {
        def source = doc.dataAsMap
        def result = conversion.revert(source)
        log.trace("Created frame: $result")

        return whelk.createDocument("application/x-marc-json").withIdentifier(doc.identifier).withData(mapper.writeValueAsBytes(result)).withEntry(doc.entry).withMeta(doc.meta)
    }
}
