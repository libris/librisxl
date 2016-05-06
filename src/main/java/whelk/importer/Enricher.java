package whelk.importer;

import com.github.jsonldjava.core.RDFDataset;
import com.google.common.collect.Lists;
import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.JsonLd;
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
        System.out.println("Enrich on: " + id);

        Document originalDocument = m_postgreSQLComponent.load(id);
        //Map contextMap = m_mapper.readValue(m_jsonldContext, HashMap.class);

        List<String[]> withTriples = JsonldSerializer.deserialize(withDocument.getData());
        List<String[]> originalTriples = JsonldSerializer.deserialize(originalDocument.getData());

        /*for (String[] triple : triples)
        {
            System.out.println(triple[0] + " -> " + triple[1] + " -> " + triple[2]);
        }*/

        Graph originalGraph = new Graph(originalTriples);
        Graph withGraph = new Graph(withTriples);
        //Graph subgraph = graph.getSubGraphFrom("https://libris.kb.se/"+withDocument.getId()+"#it");
        //graph.render();

        System.out.println(originalGraph);
        System.out.println(withGraph);

        Map<String, String> BNodeMapping = withGraph.generateBNodeMapFrom(originalGraph);

        System.out.println("BNODE MAPPINGS:");
        for (String withBNode : BNodeMapping.keySet())
        {
            String bnode = BNodeMapping.get(withBNode);
            System.out.println("with: " + withBNode + " -> " + bnode);
        }

        //Map reverted = JsonldSerializer.serialize(triples);
        /*
        {
            String rev_string = m_mapper.writeValueAsString(reverted);
            System.out.println(rev_string);
        }*/

        //reverted = JsonLd.frame(withDocument.getId(), reverted);
        //reverted = JsonLd.flatten(reverted);
        /*{
            String rev_string = m_mapper.writeValueAsString(reverted);
            System.out.println("Generated jsonld:\n" + rev_string);
        }*/

        /*
        {
            Map reference = JsonLd.flatten(withDocument.getData());
            String rev_string = m_mapper.writeValueAsString(reference);
            System.out.println("Reference jsonld:\n" + rev_string);
        }*/



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
