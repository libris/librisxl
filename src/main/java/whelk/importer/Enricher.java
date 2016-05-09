package whelk.importer;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;
import whelk.component.PostgreSQLComponent;

import java.io.IOException;
import java.util.*;

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

        //System.out.println(originalGraph);
        //System.out.println(withGraph);

        Map<String, String> bNodeMapping = withGraph.generateBNodeMapFrom(originalGraph);
        Set<String> mergedTriples = new HashSet<>(); // Just to make sure we don't introduce doubles of triples.
        for (String[] triple : originalTriples)
            mergedTriples.add(triple[0]+triple[1]+triple[2]);
        for (String[] triple : withTriples)
        {
            String subject = getTranslatedNodeId(triple[0], bNodeMapping);
            String predicate = triple[1];
            String object = getTranslatedNodeId(triple[2], bNodeMapping);

            if (subject != null && object != null)
            {
                String setRepresentation = subject+predicate+object;
                if (!mergedTriples.contains(setRepresentation))
                {
                    mergedTriples.add(setRepresentation);
                    originalTriples.add(new String[]{subject, predicate, object});
                }
            }
        }

        /*System.out.println("BNODE MAPPINGS:");
        for (String withBNode : BNodeMapping.keySet())
        {
            String bnode = BNodeMapping.get(withBNode);
            System.out.println("with: " + withBNode + " -> " + bnode);
        }*/

        Map reverted = JsonldSerializer.serialize(originalTriples);
        {
            String rev_string = m_mapper.writeValueAsString(reverted);
            System.out.println(rev_string);
        }

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

    /**
     * Given a node id, get the corresponding node id in this graph (which is the same unless it is a blank node id).
     */
    private String getTranslatedNodeId(String naiveNodeId, Map<String, String> bNodeMapping)
    {
        if (naiveNodeId.startsWith(JsonldSerializer.BLANKNODE_PREFIX))
        {
            if (bNodeMapping.keySet().contains(naiveNodeId))
                return bNodeMapping.get(naiveNodeId);
            return null;
        }
        return naiveNodeId;
    }
}
