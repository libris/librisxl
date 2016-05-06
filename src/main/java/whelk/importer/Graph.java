package whelk.importer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

public class Graph
{
    /**
     * The adjacency list (map).
     *
     * Mapping node IDs (A) to lists of predicate-subject pairs to which the (A) is connected
     */
    private Map<String, List<String[]>> edgesFromId;

    public Graph(List<String[]> triples)
    {
        edgesFromId = new HashMap<>();

        for (String[] triple : triples)
        {
            // for clarity:
            String subject = triple[0];
            String predicate = triple[1];
            String object = triple[2];

            // get the edge list for this node (subject)
            List<String[]> edges = edgesFromId.get(subject);
            if (edges == null)
            {
                edges = new ArrayList<>();
                edgesFromId.put(subject, edges);
            }

            // add the edge represented by this triple
            edges.add(new String[]{predicate, object});
        }
    }

    /**
     * Generates the subgraph extending from a given node in this graph.
     */
    public Graph getSubGraphFrom(String subject)
    {
        List<String[]> triples = new ArrayList<>();
        buildSubgraph(subject, triples);
        return new Graph(triples);
    }

    private void buildSubgraph(String subject, List<String[]> triples)
    {
        List<String[]> edges = edgesFromId.get(subject);
        if (edges == null)
            return;

        for (String[] edge : edges)
        {
            String[] triple = new String[] {subject, edge[0], edge[1]};
            triples.add(triple);

            buildSubgraph(edge[1], triples);
        }
    }

    public String toString()
    {
        StringBuilder result = new StringBuilder();
        for ( String subject : edgesFromId.keySet() )
        {
            List<String[]> edges = edgesFromId.get(subject);
            if (edges == null)
                continue;

            for (String[] edge : edges)
            {
                result.append(subject + " [" + edge[0] + " ->] " + edge[1] + "\n");
            }
        }

        return result.toString();
    }
}
