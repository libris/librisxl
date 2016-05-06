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
     * Tests if this graph is a superset of the supplied graph.
     */
    public boolean contains(Graph subgraph)
    {
        for ( String subgraphSubject : subgraph.edgesFromId.keySet() )
        {
            /*
            if ( ! containsSubjectSubgraph(subgraphSubject, subgraph) )
                return false;
                */


            /*
            List<String[]> edges = edgesFromId.get(subgraphSubject);
            List<String[]> subgraphEdges = subgraph.edgesFromId.get(subgraphSubject);

            for (String[] subgraphEdge : subgraphEdges)
            {
                for (String[] edge : )
            }
            */
        }
        return true;
    }

    /**
     * Is there a (blank) node in this graph with the equivalent of the supplied edges?
     */
    /*
    private boolean existsSuchANode(List<String[]> candidateEdges)
    {
        for (String[] candidateEdge : candidateEdges)
        {
            boolean equivalentFound = false;

            for ( String subject : edgesFromId.keySet() )
            {
                List<String[]> edges = edgesFromId.get(subject);
                for (String[] edge : edges)
                {
                    if (candidateEdge[0].equals(edge[0]) && // predicate is the same and
                            (
                                    // object is the same,
                                    candidateEdge[1].equals(edge[1]) ||
                                            // or both objects are blank nodes
                                            (
                                                    candidateEdge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX) &&
                                                    edge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX))
                                                    // and
                                            )
                            )
                    {
                        equivalentFound = true;
                        break;
                    }
                }
            }

            if (!equivalentFound)
                return false;
        }
        return true;
    }*/

    /**
     * Generates a map where BNode ids in otherGraph (assumed to be a subgraph) are mapped to BNode ids in this map
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
        List<String[]> otherEdges = otherGraph.edgesFromId.get(otherSubject);
        if (otherEdges == null)
            return true; // nothing more to check
        List<String[]> edges = edgesFromId.get(subject);
        if (edges == null)
            return false; // no more edges in (possibly) containing graph to compare with

        for (String[] otherEdge : otherEdges)
        {
            // Does each edge have an equivalent in this graph
            for (String[] edge : edges)
            {
                boolean hasEquivalentEdges =
                        edge[0].equals(otherEdge[0]) && // predicate is the same and
                        ( (edge[1].equals(otherEdge[1]) && !edge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX) ) // object is the same (and not blank node),
                                || // or both are blank nodes, and in turn have equivalent edges
                                (
                                        edge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX) && otherEdge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX) &&
                                        hasEquivalentEdges(edge[1], otherEdge[1], otherGraph)
                                ));
                if (!hasEquivalentEdges)
                    return false;
            }
        }
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
