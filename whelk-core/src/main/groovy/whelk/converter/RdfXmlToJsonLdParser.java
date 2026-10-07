package whelk.converter;

import java.util.*;
import java.io.*;

import org.apache.jena.rdf.model.ModelFactory;

import static whelk.util.Jackson.mapper;

public class RdfXmlToJsonLdParser {

    public static Object parse(InputStream inStream) throws IOException {
        return parse(inStream, (Map) null);
    }

    public static Object parse(InputStream inStream, Map context) throws IOException {
        return parse(inStream, context, null);
    }

    public static Object parse(InputStream inStream, Map context, String baseIri) throws IOException {
        var model = ModelFactory.createDefaultModel();
        model.read(inStream, baseIri, "RDF/XML");
        var baos = new ByteArrayOutputStream();
        try (var outSteam = new OutputStreamWriter(baos)) {
          model.write(outSteam, "JSONLD");
        }
        var inData = mapper.readValue(baos.toString("UTF-8"), Map.class);
        var data = JsonLdShapes.reCompactWithEmbeddedBlanks(inData, context, baseIri);
        return data;
    }

}
