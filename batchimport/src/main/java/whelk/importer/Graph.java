package whelk.importer;

import java.util.*;

public class Graph
{
    /**
     * When enriching one graph with another, sometimes special rules need to be applied to certain predicates, in order
     * to maintain the domain-specific correctness of the resulting graph.
     * For example a Whelk document should have only one "created"-timestamp, while the graphs being merged will
     * typically have one each, and so the special rule RULE_PREFER_ORIGINAL should be used.
     *
     * All predicates for which there are no special rules will be considered belonging to the aggregate category.
     * Meaning edges with such predicates will be aggregated side by side (in a list in json-ld terms).
     */
    public enum PREDICATE_RULES
    {
        RULE_PREFER_ORIGINAL, // Only one such predicate per subject, prefer the original documents version to the incoming
        RULE_PREFER_INCOMING, // Only one such predicate per subject, prefer the incoming documents version to the original (= overwrite)
    };

    /**
     * The adjacency list (map).
     *
     * Mapping node IDs (A) to lists of predicate-subject pairs (edges) that connect (A) with other nodes
     */
    private Map<String, List<String[]>> m_edgesFromId;

    private int m_currentHighestBNodeID = 0;

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
    }

    public void enrichWith(Graph otherGraph, Map<String, PREDICATE_RULES> specialRules)
    {
        Map<String, String> bNodeMapping = otherGraph.generateBNodeMapTo(this);

        m_currentHighestBNodeID = getCurrentHighestBNodeID();

        for (String subject : otherGraph.m_edgesFromId.keySet())
        {
            List<String[]> otherEdges = otherGraph.m_edgesFromId.get(subject);
            subject = getTranslatedNodeId(subject, bNodeMapping);
            for (String[] otherEdge: otherEdges)
            {
                String predicate = otherEdge[0];
                String object = getTranslatedNodeId(otherEdge[1], bNodeMapping);

                if (specialRules.get(predicate) == null) // default, AGGREGATE
                {
                    addTriple(new String[]{subject, predicate, object});
                }
                else if (specialRules.get(predicate) == PREDICATE_RULES.RULE_PREFER_ORIGINAL)
                {
                    // Add the triple only if we don't already have such a subject-predicate pair.
                    int occurrences = subjectPredicatePairOccurrences(subject, predicate);
                    if (occurrences == 0)
                        addTriple(new String[]{subject, predicate, object});
                }
                else if (specialRules.get(predicate) == PREDICATE_RULES.RULE_PREFER_INCOMING)
                {
                    // delete any preexisting edges in this graph that match this subject-predicate pair.
                    List<String[]> subjectEdges = m_edgesFromId.get(subject);
                    Iterator<String[]> it = subjectEdges.iterator();
                    while (it.hasNext())
                    {
                        String[] edge = it.next();
                        if (edge[0].equals(predicate))
                            it.remove();
                    }

                    addTriple(new String[]{subject, predicate, object});
                }
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
     * Is there such an edge in this graph, that 'subject' is connected to some (arbitrary)
     * other node via an edge of type 'predicate' ?
     * Returns the number of such edges.
     */
    private int subjectPredicatePairOccurrences(String subject, String predicate)
    {
        List<String[]> edges = m_edgesFromId.get(subject);
        int result = 0;
        for (String[] edge : edges)
        {
            if (predicate.equals(edge[0]))
                ++result;
        }
        return result;
    }

    /**
     * Find an integer I, such that new BNodes _:b(I+N) for N=1,2,3.. can be safely (without risk of collision) added to this graph
     */
    private int getCurrentHighestBNodeID()
    {
        int currentHighestBNodeID = 0;
        for (String subject : m_edgesFromId.keySet())
        {
            List<String[]> edges = m_edgesFromId.get(subject);
            for (String[] edge: edges)
            {
                String predicate = edge[0];
                String object = edge[1];

                try
                {
                    int bnodeNumber = Integer.parseInt(subject.substring( JsonldSerializer.BLANKNODE_PREFIX.length() ));
                    if (bnodeNumber >= currentHighestBNodeID)
                        currentHighestBNodeID = bnodeNumber;
                }
                catch (Exception e) { /* ignore */ }
            }
        }
        return  currentHighestBNodeID;
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
            String newBNodeId = JsonldSerializer.BLANKNODE_PREFIX + (++m_currentHighestBNodeID);
            bNodeMapping.put(naiveNodeId, newBNodeId);
            return newBNodeId;
        }
        return naiveNodeId;
    }

    /**
     * Generates a map where BNode ids in this graph are mapped to BNode ids in otherGraph (where the mapping can be
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
     * (are the edges of otherGraph[otherSubject] a subset of the edges of this[subject])
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

        // Compare edges
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
