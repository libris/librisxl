package whelk.converter;

import java.util.*;
import java.io.*;

import whelk.exception.WhelkRuntimeException;

import static whelk.JsonLd.CONTEXT_KEY;
import static whelk.JsonLd.GRAPH_KEY;
import static whelk.util.Jackson.mapper;

public class RdfReader {

    static Map readRdf(InputStream bis, String rdfSourcePath, Map context) throws IOException {
        Map data;
        if (rdfSourcePath.endsWith(".ttl")) {
            data = readTurtle(bis, rdfSourcePath, context);
        } else if (rdfSourcePath.endsWith(".rdf")) {
            data = readRdfXml(bis, rdfSourcePath, context);
        } else if (rdfSourcePath.endsWith(".jsonld")) {
            data = readJsonLd(bis, rdfSourcePath, context);
        } else {
            throw new WhelkRuntimeException("Unknown RDF format for ${rdfSourcePath}");
        }

        if (!data.containsKey(GRAPH_KEY)) {
            var wrapper = new HashMap<String, Object>();
            if (data.containsKey(CONTEXT_KEY)) {
                wrapper.put(CONTEXT_KEY, data.remove(CONTEXT_KEY));
            }
            var nodes = new ArrayList<Map>();
            nodes.add(data);
            wrapper.put(GRAPH_KEY, nodes);
            return wrapper;
        } else {
            return data;
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
