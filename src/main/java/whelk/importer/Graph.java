package whelk.importer;

import java.util.*;

public class Graph
{
    /**
     * The adjacency list (map).
     *
     * Mapping node IDs (A) to lists of predicate-subject pairs (edges) that connect (A) with other nodes
     */
    private Map<String, List<String[]>> m_edgesFromId;

    /**
     * Keep track of the highest currently assigned BNode id in the graph (so that new BNode ids can be assigned)
     */
    private int m_nextBNodeIDNumber = 0;

    public Graph(List<String[]> triples)
    {
        m_edgesFromId = new HashMap<>();

        for (String[] triple : triples)
        {
            addTriple(triple);
        }
    }

    /**
     * Add the given triple to this graph
     */
    public void addTriple(String[] triple)
    {
        // for clarity:
        String subject = triple[0];
        String predicate = triple[1];
        String object = triple[2];

        // get the edge list for this node (subject)
        List<String[]> edges = m_edgesFromId.get(subject);
        if (edges == null)
        {
            edges = new ArrayList<>();
            m_edgesFromId.put(subject, edges);
        }

        // check for duplicates
        for (String[] edge : edges)
        {
            if (edge[0].equals(predicate) && edge[1].equals(object)) // We already have this edge!
                return;
        }

        // add the edge represented by this triple
        edges.add(new String[]{predicate, object});

        // keep track of the highest BNode id
        if (subject.startsWith(JsonldSerializer.BLANKNODE_PREFIX))
        {
            // There may be BNode ids that are formatted in other ways and may fail the below check. But Such IDs are
            // of no consequence as they will for the same reason not interfere with new BNode IDs we assign later on.
            try
            {
                int bnodeNumber = Integer.parseInt(subject.substring( JsonldSerializer.BLANKNODE_PREFIX.length() ));
                if (bnodeNumber >= m_nextBNodeIDNumber)
                    m_nextBNodeIDNumber = bnodeNumber + 1;
            }
            catch (Exception e) { /* ignore */ }
        }
    }

    public void enrichWith(Graph otherGraph)
    {
        Map<String, String> bNodeMapping = otherGraph.generateBNodeMapTo(this);

        for (String subject : otherGraph.m_edgesFromId.keySet())
        {
            List<String[]> otherEdges = otherGraph.m_edgesFromId.get(subject);
            subject = getTranslatedNodeId(subject, bNodeMapping);
            for (String[] otherEdge: otherEdges)
            {
                String predicate = otherEdge[0];
                String object = getTranslatedNodeId(otherEdge[1], bNodeMapping);
                addTriple(new String[]{subject, predicate, object});
            }
        }
    }

    /**
     * Get all triples in this graph
     */
    public List<String[]> getTriples()
    {
        List<String[]> triples = new ArrayList<>();

        for (String subject : m_edgesFromId.keySet())
        {
            List<String[]> edges = m_edgesFromId.get(subject);
            for (String[] edge: edges)
            {
                String predicate = edge[0];
                String object = edge[1];
                triples.add(new String[]{subject, predicate, object});
            }
        }

        return triples;
    }

    public String toString()
    {
        StringBuilder result = new StringBuilder();
        for ( String subject : m_edgesFromId.keySet() )
        {
            List<String[]> edges = m_edgesFromId.get(subject);
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


    /**
     * Given a node id in the graph from which bNodeMapping is derived, get the corresponding node id in this graph
     * (which is usually the same unless it is a blank node id).
     */
    private String getTranslatedNodeId(String naiveNodeId, Map<String, String> bNodeMapping)
    {
        if (naiveNodeId.startsWith(JsonldSerializer.BLANKNODE_PREFIX))
        {
            if (bNodeMapping.keySet().contains(naiveNodeId))
                return bNodeMapping.get(naiveNodeId);

            // A blank node without mapping? Give it a new blank node id in this graph
            // and add the mapping, so the linked web remains intact
            String newBNodeId = JsonldSerializer.BLANKNODE_PREFIX + m_nextBNodeIDNumber++;
            bNodeMapping.put(naiveNodeId, newBNodeId);
            return newBNodeId;
        }
        return naiveNodeId;
    }

    /**
     * Generates a map where BNode ids in otherGraph are mapped to BNode ids in this map (where the mapping can be
     * done with surety)
     */
    private Map<String, String> generateBNodeMapTo(Graph otherGraph)
    {
        Map<String, String> BNodeMap = new HashMap<>();
        for (String otherSubject : otherGraph.m_edgesFromId.keySet())
        {
            if (otherSubject.startsWith(JsonldSerializer.BLANKNODE_PREFIX))
            {
                for (String subject : m_edgesFromId.keySet())
                {
                    if (subject.startsWith(JsonldSerializer.BLANKNODE_PREFIX))
                    {
                        // subject and otherSubject are both blank nodes.
                        if (hasEquivalentEdges(subject, otherSubject, otherGraph, new HashMap<>(), new HashMap<>()))
                            BNodeMap.put(subject, otherSubject);
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
        List<String[]> otherEdges = otherGraph.m_edgesFromId.get(otherSubject);
        if (otherEdges == null)
            return true; // nothing more to check
        List<String[]> edges = m_edgesFromId.get(subject);
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
}
