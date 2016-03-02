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

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

class JsonLD2RdfXml implements FormatConverter {

    static final ObjectMapper mapper = new ObjectMapper()

    Map m_context = null;

    public Document convert(Document doc)
    {
        readContextFromDb();

        Map originalData = doc.getData();
        Map framed = JsonLd.frame(doc.getId(), originalData);
        framed.putAll(m_context);
        String framedString = mapper.writeValueAsString(framed);

        InputStream input = IOUtils.toInputStream(framedString);
        Model model = ModelFactory.createDefaultModel();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        model = model.read(input, Document.BASE_URI.toString(), "JSONLD");
        RDFWriter writer = model.getWriter("RDF/XML")
        writer.setProperty("allowBadURIs","true")
        writer.write(model, baos, Document.BASE_URI.toString())

        HashMap<String, String> data = new HashMap<String, String>();
        data.put( Document.NON_JSON_CONTENT_KEY, baos.toString("UTF-8") );
        Document converted = new Document(doc.getId(), data, doc.getManifest());
        return converted;
    }

    public String getRequiredContentType()
    {
        return "application/ld+json";
    }

    public String getResultContentType()
    {
        return "application/rdf+xml";
    }
    
    private synchronized readContextFromDb()
    {
        if (m_context == null)
        {
            Properties props = PropertyLoader.loadProperties("secret")
            PostgreSQLComponent postgreSQLComponent = new PostgreSQLComponent(props.getProperty("sqlUrl"), props.getProperty("sqlMaintable"));

            // select data from lddb where data#>>'{@graph,0,@id}' = 'https://id.kb.se/vocab/context'
            // OR
            // select data from lddb where id in (select id from lddb__identifiers where identifier = 'https://id.kb.se/vocab/context')

            // The data#>>'{@graph,0}' should really be just data, but must be there as a temporary workaround
            String sql = "select data#>>'{@graph,0}' from lddb where id in (select id from lddb__identifiers where identifier = 'https://id.kb.se/vocab/context')";

            // groovy does not support try-with-resources ..
            Connection connection;
            try {
                connection = postgreSQLComponent.getConnection()
                PreparedStatement preparedStatement
                try {
                    preparedStatement = connection.prepareStatement(sql)
                    ResultSet resultSet;
                    try {

                        resultSet = preparedStatement.executeQuery();
                        if (resultSet.next())
                        {
                            String contextString = resultSet.getString(1);
                            m_context = mapper.readValue(contextString, HashMap.class);

                            //System.out.println(contextString);
                        }

                    }finally {
                        if (resultSet != null)
                            resultSet.close();
                    }
                } finally {
                    if (preparedStatement != null)
                        preparedStatement.close();
                }
            } finally {
                if (connection != null)
                    connection.close();
            }

        }
    }
}
