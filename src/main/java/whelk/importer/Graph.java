package whelk.importer;

import java.util.*;

public class Graph
{
    /**
     * The adjacency list (map).
     *
     * Mapping node IDs (A) to lists of predicate-subject pairs (edges) that connect (A) with other nodes
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
     * Generates a map where BNode ids in otherGraph are mapped to BNode ids in this map (where the mapping can be
     * done with surety)
     */
    public Map<String, String> generateBNodeMapFrom(Graph otherGraph)
    {
        Map<String, String> BNodeMap = new HashMap<>();
        for (String otherSubject : otherGraph.edgesFromId.keySet())
        {
            if (otherSubject.startsWith(JsonldSerializer.BLANKNODE_PREFIX))
            {
                for (String subject : edgesFromId.keySet())
                {
                    if (subject.startsWith(JsonldSerializer.BLANKNODE_PREFIX))
                    {
                        // subject and otherSubject are both blank nodes.
                        if (hasEquivalentEdges(subject, otherSubject, otherGraph, new HashMap<>(), new HashMap<>()))
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
    private boolean hasEquivalentEdges(String subject, String otherSubject, Graph otherGraph, Map<String,
            String> cycleGuard, Map<String, String> otherCycleGuard)
    {
        // Check for going in circles
        if ( cycleGuard.keySet().contains(subject) && otherCycleGuard.keySet().contains(otherSubject) )
        {
            // Cycle detected in both graphs. Is the other graph in the same position it was the last time we were here?
            if ( cycleGuard.get(subject).equals(otherSubject) && otherCycleGuard.get(otherSubject).equals(subject) )
                return true;
        }
        if (cycleGuard.keySet().contains(subject) || otherCycleGuard.keySet().contains(otherSubject))
            return false; // We've been here already in one graph, but the position in the other graph was different then.
        cycleGuard.put(subject, otherSubject);
        otherCycleGuard.put(otherSubject, subject);

        // Normal recursive base cases
        List<String[]> otherEdges = otherGraph.edgesFromId.get(otherSubject);
        if (otherEdges == null)
            return true; // nothing more to check
        List<String[]> edges = edgesFromId.get(subject);
        if (edges == null)
            return false; // no more edges in (possibly) containing graph to compare with

        // Compare the edges from this node (with the other graph)
        for (String[] otherEdge : otherEdges)
        {
            boolean hasEquivalentEdges = false;

            // Does each 'otherEdge' have an equivalent edge in this graph ?
            for (String[] edge : edges)
            {
                boolean edgesMatch =
                        edge[0].equals(otherEdge[0]) && // predicate is the same and
                        ( (edge[1].equals(otherEdge[1]) && !edge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX) ) // object is the same (and not a blank node),
                                || // or both objects are blank nodes, and in turn have equivalent edges
                                (
                                        edge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX) && otherEdge[1].startsWith(JsonldSerializer.BLANKNODE_PREFIX) &&
                                        hasEquivalentEdges(edge[1], otherEdge[1], otherGraph, cycleGuard, otherCycleGuard)
                                ));
                if (edgesMatch)
                    hasEquivalentEdges = true;
            }

            if (!hasEquivalentEdges)
                return false;
        }
        return true;
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
                result.append(subject);
                result.append(" [");
                result.append(edge[0]);
                result.append(" ->] ");
                result.append(edge[1]);
                result.append("\n");
            }
        }

        return result.toString();
    }
}
