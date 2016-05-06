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

    /**********************************/



    /**
     * Generates a map where BNode ids in otherGraph are mapped to BNode ids in this map (where the mapping can be
     * done with surety)
     */
    public Map generateBNodeMapFrom(Graph otherGraph)
    {
        Map BNodeMap = new HashMap<>();
        for (String otherSubject : otherGraph.edgesFromId.keySet())
        {
            if (otherSubject.startsWith(JsonldSerializer.BLANKNODE_PREFIX))
            {
                for (String subject : edgesFromId.keySet())
                {
                    if (subject.startsWith(JsonldSerializer.BLANKNODE_PREFIX))
                    {
                        // subject and otherSubject are both blank nodes.
                        if (hasEquivalentEdges(subject, otherSubject, otherGraph))
                            BNodeMap.put(otherSubject, subject);
                    }
                }
            }
        }
        return BNodeMap;
    }

    /**
     * Does 'subject' have the equivalent edges in this graph, compared to 'otherSubject' in otherGraph ?
     */
    private boolean hasEquivalentEdges(String subject, String otherSubject, Graph otherGraph)
    {
        System.out.println("Called to compare " + subject + " to " + otherSubject);
        List<String[]> otherEdges = otherGraph.edgesFromId.get(otherSubject);
        if (otherEdges == null)
        {
            System.out.println("ret true");
            return true; // nothing more to check
        }
        List<String[]> edges = edgesFromId.get(subject);
        if (edges == null)
        {
            System.out.println("ret false0");
            return false; // no more edges in (possibly) containing graph to compare with
        }

        for (String[] otherEdge : otherEdges)
        {
            boolean hasEquivalentEdges = false;

            // Does each 'otherEdge' have an equivalent in this graph ?
            for (String[] edge : edges)
            {
                System.out.println("\tEdgecompare, predicates: " + edge[0] + ", " + otherEdge[0]);
                boolean edgesMatch =
                        edge[0].equals(otherEdge[0]) && // predicate is the same and
                        ( (edge[1].equals(otherEdge[1]) && !edge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX) ) // object is the same (and not a blank node),
                                || // or both objects are blank nodes, and in turn have equivalent edges
                                (
                                        edge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX) && otherEdge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX) &&
                                        hasEquivalentEdges(edge[1], otherEdge[1], otherGraph)
                                ));
                if (edgesMatch)
                {
                    System.out.println("\t\tmatch.");
                    hasEquivalentEdges = true;
                }
            }

            if (!hasEquivalentEdges)
            {
                System.out.println("ret false1");
                return false;
            }
        }
        System.out.println("ret true");
        return true;
    }

    /**********************************/

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
