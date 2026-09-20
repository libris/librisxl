package whelk.converter;

import java.util.*;
import java.io.*;

import whelk.exception.WhelkRuntimeException;
import static whelk.util.Jackson.mapper;

public class RdfReader {

    static Map readRdf(InputStream bis, String rdfSourcePath, Map context) throws IOException {
      if (rdfSourcePath.endsWith(".ttl")) {
        return readTurtle(bis, rdfSourcePath, context);
      } else if (rdfSourcePath.endsWith(".rdf")) {
        return readRdfXml(bis, rdfSourcePath, context);
      } else if (rdfSourcePath.endsWith(".jsonld")) {
        return readJsonLd(bis, rdfSourcePath, context);
      } else {
        throw new WhelkRuntimeException("Unknown RDF format for ${rdfSourcePath}");
      }
    }

    static Map readTurtle(InputStream bis, String rdfSourcePath, Map context) throws IOException {
        return TrigToJsonLdParser.parse(bis, context);
    }

    static Map readRdfXml(InputStream bis, String rdfSourcePath, Map context) throws IOException {
        return (Map) RdfXmlToJsonLdParser.parse(bis, context);
    }

    static Map readJsonLd(InputStream bis, String rdfSourcePath, Map context) throws IOException {
        Map data = mapper.readValue(bis, Map.class);
        return (Map) TrigToJsonLdParser.compact(data, context);
    }

}
