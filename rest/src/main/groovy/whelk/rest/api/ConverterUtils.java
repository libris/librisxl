package whelk.rest.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.converter.FormatConverter;
import whelk.converter.JsonLD2N3Converter;
import whelk.converter.JsonLD2RdfXml;
import whelk.converter.JsonLDTrigConverter;
import whelk.converter.JsonLDTurtleConverter;
import whelk.util.http.MimeTypes;

import java.util.HashMap;
import java.util.Map;

public class ConverterUtils {
    private static final Logger log = LogManager.getLogger(ConverterUtils.class);

    private final Whelk whelk;
    private final Map<String, FormatConverter> converters;

    public ConverterUtils(Whelk whelk) {
        this.whelk = whelk;

        converters = new HashMap<>();
        converters.put(MimeTypes.RDF, new JsonLD2RdfXml(whelk));
        converters.put(MimeTypes.TURTLE, new JsonLDTurtleConverter(null, whelk));
        converters.put(MimeTypes.TRIG, new JsonLDTrigConverter(null, whelk));
        converters.put(MimeTypes.N3, new JsonLD2N3Converter(whelk));
    }

    public String convert(Object source, String id, String contentType) {
        FormatConverter converter = converters.get(contentType);
        if (converter == null) {
            throw new IllegalArgumentException("No converter found for content type: " + contentType);
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> sourceMap = (Map<String, Object>) source;
        Map<String, Object> result = converter.convert(sourceMap, id);

        return (String) result.get(JsonLd.NON_JSON_CONTENT_KEY);
    }
}
