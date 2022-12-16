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

    Map convert(Map originaldata, String id) {
        var srcData = [:]
        srcData.putAll(originaldata)
        if (context) {
            srcData[JsonLd.CONTEXT_KEY] = context
        }

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

    String getRequiredContentType() {
        return "application/ld+json"
    }

    String getResultContentType() {
        return "application/rdf+xml"
    }
}
