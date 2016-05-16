package whelk.importer;
import whelk.Document;

import java.io.IOException;
import java.util.*;

public class Enricher
{
    public static void enrich(Document mutableDocument, Document withDocument)
            throws IOException
    {
        List<String[]> withTriples = JsonldSerializer.deserialize(withDocument.getData());
        List<String[]> originalTriples = JsonldSerializer.deserialize(mutableDocument.getData());

        Graph originalGraph = new Graph(originalTriples);
        Graph withGraph = new Graph(withTriples);

        //System.out.println(originalGraph);
        //System.out.println(withGraph);

        originalGraph.enrichWith(withGraph);

        Map enrichedData = JsonldSerializer.serialize(originalGraph.getTriples());
        JsonldSerializer.normalize(enrichedData, mutableDocument.getId());
        mutableDocument.setData(enrichedData);
    }
}
