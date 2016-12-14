package whelk.converter

import org.apache.commons.io.IOUtils
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.RDFWriter
import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.JsonLd
import whelk.component.PostgreSQLComponent
import whelk.util.PropertyLoader

class JsonLD2RdfXml implements FormatConverter {

    static final ObjectMapper mapper = new ObjectMapper()

    Map m_context = null

    Map convert(Map originaldata, String id) {
        readContextFromDb()
        Map framed = JsonLd.frame(id, originaldata)

        framed.putAll(m_context)
        String framedString = mapper.writeValueAsString(framed)

        InputStream input = IOUtils.toInputStream(framedString)
        Model model = ModelFactory.createDefaultModel()
        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        model = model.read(input, Document.BASE_URI.toString(), "JSONLD")
        RDFWriter writer = model.getWriter("RDF/XML")
        writer.setProperty("allowBadURIs","true")
        writer.write(model, baos, Document.BASE_URI.toString())

        HashMap<String, String> data = new HashMap<String, String>()
        data.put(JsonLd.NON_JSON_CONTENT_KEY, baos.toString("UTF-8"))
        return data
    }

    public String getRequiredContentType() {
        return "application/ld+json"
    }

    public String getResultContentType() {
        return "application/rdf+xml"
    }

    private synchronized readContextFromDb() {
        if (m_context == null) {
            Properties props = PropertyLoader.loadProperties("secret")
            PostgreSQLComponent postgreSQLComponent = new PostgreSQLComponent(props.getProperty("sqlUrl"), props.getProperty("sqlMaintable"))
            Map context = mapper.readValue(postgreSQLComponent.getContext(), HashMap.class)
            m_context = [:]
            m_context[JsonLd.CONTEXT_KEY] = context[JsonLd.CONTEXT_KEY]
        }
    }
}
