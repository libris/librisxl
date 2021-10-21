package whelk.converter

import org.apache.commons.io.IOUtils
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.RDFWriter
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.util.PropertyLoader

import static whelk.util.Jackson.mapper

class JsonLD2N3Converter implements FormatConverter {

    Map m_context = null

    JsonLD2N3Converter(Whelk whelk = null) {
        if (whelk) {
            m_context = [:]
            m_context[JsonLd.CONTEXT_KEY] = whelk.jsonld.context
        }
    }

    Map convert(Map originaldata, String id) {
        readContextFromDb()
        Map framed = JsonLd.frame(Document.BASE_URI.resolve(id).toString(), originaldata)

        framed.putAll(m_context)
        String framedString = mapper.writeValueAsString(framed)

        InputStream input = IOUtils.toInputStream(framedString)
        Model model = ModelFactory.createDefaultModel()
        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        model = model.read(input, Document.BASE_URI.toString(), "JSONLD")
        RDFWriter writer = model.getWriter("N3")
        writer.setProperty("allowBadURIs","true")
        writer.write(model, baos, "")

        HashMap<String, String> data = new HashMap<String, String>()
        data.put(JsonLd.NON_JSON_CONTENT_KEY, baos.toString("UTF-8"))
        return data
    }

    public String getRequiredContentType() {
        return "application/ld+json"
    }

    public String getResultContentType() {
        return "text/n3"
    }

    private synchronized readContextFromDb() {
        if (m_context == null) {
            Properties props = PropertyLoader.loadProperties("secret")
            PostgreSQLComponent postgreSQLComponent = new PostgreSQLComponent(props.getProperty("sqlUrl"))
            Map context = mapper.readValue(postgreSQLComponent.getContext(), HashMap.class)
            m_context = [:]
            m_context[JsonLd.CONTEXT_KEY] = context[JsonLd.CONTEXT_KEY]
        }
    }
}
