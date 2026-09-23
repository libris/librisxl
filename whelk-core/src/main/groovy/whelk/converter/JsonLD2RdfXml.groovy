package whelk.converter

import groovy.transform.CompileStatic

import org.apache.commons.io.IOUtils
import org.apache.jena.rdf.model.ModelFactory
import whelk.Document
import whelk.JsonLd
import whelk.Whelk

import java.nio.charset.StandardCharsets

import static whelk.util.Jackson.mapper

@CompileStatic
class JsonLD2RdfXml implements FormatConverter {

    private Map context = null

    JsonLD2RdfXml(Whelk whelk = null) {
       if (whelk) {
           context = whelk.jsonld.context
       }
    }

    // Characters that can't be represented in XML 1.0 in any form
    private static final java.util.regex.Pattern NON_XML_CHARS =
            ~/[\x00-\x08\x0B\x0C\x0E-\x1F\uFFFE\uFFFF]/

    Map convert(Map originaldata, String id) {
        var srcData = [:]
        srcData.putAll(originaldata)
        if (context && JsonLd.CONTEXT_KEY !in srcData) {
            srcData[JsonLd.CONTEXT_KEY] = context
        }

        // Strip *before* serializing because Jackson escapes control characters
        // into their \uXXXX escape form in the JSON text, and Jena would then
        // parse those \uXXXX back into (invalid) characters.
        srcData = (Map) stripNonXmlChars(srcData)

        var jsonldStr = mapper.writeValueAsString(srcData)
        var input = IOUtils.toInputStream(jsonldStr, StandardCharsets.UTF_8)
        var baos = new ByteArrayOutputStream()
        var model = ModelFactory.createDefaultModel()
        model = model.read(input, Document.BASE_URI.toString(), "JSONLD")
        var writer = model.getWriter("RDF/XML")
        writer.setProperty("allowBadURIs","true")
        writer.write(model, baos, "")

        var data = new HashMap<String, String>()
        data.put(JsonLd.NON_JSON_CONTENT_KEY, baos.toString("UTF-8"))

        return data
    }

    /**
     * Recursively remove characters that cannot be represented in XML from the
     * string values of a parsed JSON-LD structure.
     */
    private static Object stripNonXmlChars(Object o) {
        switch (o) {
            case String:
                var s = (String) o
                return NON_XML_CHARS.matcher(s).replaceAll('')
            case Map:
                var out = new LinkedHashMap()
                ((Map) o).each { k, v ->
                    out[k] = stripNonXmlChars(v)
                }
                return out
            case List:
                return ((List) o).collect { stripNonXmlChars(it) }
            default:
                return o
        }
    }

    String getRequiredContentType() {
        return "application/ld+json"
    }

    String getResultContentType() {
        return "application/rdf+xml"
    }
}
