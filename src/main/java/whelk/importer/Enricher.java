package whelk.importer;

import org.codehaus.jackson.map.ObjectMapper;
import whelk.Document;

import java.io.IOException;
import java.util.*;

public class Enricher
{
    static ObjectMapper s_mapper = new ObjectMapper();

    public static void enrich(Document mutableDocument, Document withDocument)
            throws IOException
    {
        List<String[]> withTriples = JsonldSerializer.deserialize(withDocument.getData());
        List<String[]> originalTriples = JsonldSerializer.deserialize(mutableDocument.getData());

        Graph originalGraph = new Graph(originalTriples);
        Graph withGraph = new Graph(withTriples);

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

        Map enrichedData = JsonldSerializer.serialize(originalTriples);
        mutableDocument.setData(enrichedData);
    }

    /**
     * Given a node id, get the corresponding node id in this graph (which is the same unless it is a blank node id).
     */
    private static String getTranslatedNodeId(String naiveNodeId, Map<String, String> bNodeMapping)
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
