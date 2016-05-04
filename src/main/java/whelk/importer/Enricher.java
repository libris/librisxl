package whelk.importer;

import com.github.jsonldjava.core.RDFDataset;
import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.component.PostgreSQLComponent;
import whelk.converter.JsonLD2RdfXml;
import whelk.util.Tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Enricher
{
    private final String m_jsonldContext;
    private final ObjectMapper m_mapper;
    private final PostgreSQLComponent m_postgreSQLComponent;

    public Enricher(PostgreSQLComponent postgreSQLComponent)
    {
        m_postgreSQLComponent = postgreSQLComponent;
        m_jsonldContext = m_postgreSQLComponent.getContext();
        m_mapper = new ObjectMapper();
    }

    public void enrich(String id, Document withDocument)
            throws IOException
    {
        //Map contextMap = m_mapper.readValue(m_jsonldContext, HashMap.class);

        List<String[]> triples = JsonldSerializer.deserialize(withDocument.getData());

        for (String[] triple : triples)
        {
            System.out.println(triple[0] + " -> " + triple[1] + " -> " + triple[2]);
        }

        Map reverted = JsonldSerializer.serialize(triples);
        String rev_string = m_mapper.writeValueAsString(reverted);
        System.out.println(rev_string);

        //RDFDataset rdf = Tools.toDataTriples(withDocument, contextMap);
        //Tools.fromTriples(rdf);

        /*
        m_postgreSQLComponent.storeAtomicUpdate(id, false,
                (Document doc) ->
        {

            try
            {
                Model documentModel = Tools.getDataTriples(doc, contextMap);
                //Model withModel = Tools.getDataTriples(withDocument, contextMap);

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                RDFWriter writer = documentModel.getWriter("JSON-LD");
                writer.setProperty("allowBadURIs","true");
                writer.write(documentModel, baos, Document.getBASE_URI().toString());

                doc.setData( m_mapper.readValue(baos.toString("UTF-8"), HashMap.class) );
                System.out.println("Enriched data: " + doc.getDataAsString());

            } catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }

        });
*/

    }
}
