package whelk.rest.api

import groovy.util.logging.Log4j2 as Log
import whelk.JsonLd
import whelk.Whelk
import whelk.converter.FormatConverter
import whelk.converter.JsonLD2N3Converter
import whelk.converter.JsonLD2RdfXml
import whelk.converter.JsonLDTrigConverter
import whelk.converter.JsonLDTurtleConverter
import whelk.util.http.MimeTypes

@Log
class ConverterUtils {
    Whelk whelk
    Map<String, FormatConverter> converters

    ConverterUtils(Whelk whelk) {
        this.whelk = whelk

        converters = [
                (MimeTypes.RDF)   : new JsonLD2RdfXml(whelk),
                (MimeTypes.TURTLE): new JsonLDTurtleConverter(null, whelk),
                (MimeTypes.TRIG)  : new JsonLDTrigConverter(null, whelk),
                (MimeTypes.N3)    : new JsonLD2N3Converter(whelk),
        ]
    }

    String convert(Object source, String id, String contentType) {
        return converters[contentType].convert((Map)source, id)[JsonLd.NON_JSON_CONTENT_KEY]
    }
}
