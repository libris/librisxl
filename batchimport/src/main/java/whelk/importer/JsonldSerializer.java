package whelk.importer;

import whelk.Document;
import whelk.JsonLd;

import java.util.*;

/**
 * This class serializes and deserializes RDF triples (represented as String[3]) to/from a subset of jsonld.
 */
@SuppressWarnings("unchecked")
public class JsonldSerializer
{
    public static final String LITERAL_STRING_PREFIX = "__literal_string:";
    public static final String LITERAL_INTEGER_PREFIX = "__literal_integer:";
    public static final String LITERAL_FLOAT_PREFIX = "__literal_float:";
    public static final String LITERAL_BOOLEAN_PREFIX = "__literal_boolean:";
    public static final String BLANKNODE_PREFIX = "_:b";

    // State required for deserialization (for assigning new BNode IDs)
    private int m_lastBlankNodeId = -1;

    /*******************************************************************************************************************
     * Deserialization
     ******************************************************************************************************************/

    /**
     * Deserialize a jsonld structure into a triple list.
     * If your jsonld is in string format, use something like the following line
     * to parse it into a map/list structure first:
     * Map jsonMap = new ObjectMapper().readValue(jsonString, HashMap.class);
     * The jsonld structure need not contain any context definition.
     */
    public List<String[]> deserialize(Map jsonMap)
    {
        m_lastBlankNodeId = getHighestBNodeID(jsonMap);
        List<String[]> result = new ArrayList<String[]>();
        parseMap(jsonMap, result, null);
        return result;
    }

    /**
     * Find an integer I, such that new BNodes _:b(I+N) for N=1,2,3.. can be safely added (without risk of collision).
     */
    private static int getHighestBNodeID(Map map)
    {
        int candidate = 0;
        for (Object keyObj : map.keySet())
        {
            String key = (String) keyObj;
            if (key.equals("@id") && ((String)map.get(key)).startsWith(BLANKNODE_PREFIX))
            {
                // This might throw a integer parsing exception with blank nodes that use a different naming scheme.
                // Such bnodes can be safely ignored, as they will not interfere with our bnode enumeration anyway.
                try
                {
                    String bNodeId = (String)map.get(key);
                    int bnodeNumber = Integer.parseInt(bNodeId.substring( JsonldSerializer.BLANKNODE_PREFIX.length() ));
                    candidate = Math.max(candidate, bnodeNumber);
                }
                catch (Exception e) { /* ignore */ }
            }

            Object subobject = map.get(key);
            if ( subobject instanceof Map )
                candidate = Math.max(candidate, getHighestBNodeID( (Map) subobject ));
            else if ( subobject instanceof List )
                candidate = Math.max(candidate, getHighestBNodeID( (List) subobject ));
        }
        return candidate;
    }

    private static int getHighestBNodeID(List list)
    {
        int candidate = 0;
        for (Object item : list)
        {
            if ( item instanceof Map )
                candidate = Math.max(candidate, getHighestBNodeID( (Map) item ));
        }
        return candidate;
    }

    /**
     * nodeId is the id of the node we're currently traversing, which might be a blank node id
     * (or null at the beginning).
     */
    private void parseMap(Map jsonMap, List<String[]> result, String nodeId)
    {
        for (Object keyObj : jsonMap.keySet())
        {
            String key = (String) keyObj;
            if (!key.equals("@id"))
            {
                String subject = nodeId;
                String predicate = key;
                String object = null;

                Object objectCandidate = jsonMap.get(key);
                object = getObjectIdOrLiteralValue(objectCandidate);

                // store triple
                if (subject != null && object != null)
                    result.add(new String[] {subject, predicate, object});

                // go deeper
                Object subobject = jsonMap.get(key);
                if ( subobject instanceof Map )
                    parseMap( (Map) subobject, result, object);
                else if ( subobject instanceof List )
                    parseList( (List) subobject, result, nodeId, predicate);
            }
        }
    }

    private void parseList(List jsonList, List<String[]> result, String nodeId, String predicate)
    {
        for (Object item : jsonList)
        {
            String subject = nodeId;
            String object = getObjectIdOrLiteralValue(item);

            // store triple
            if (subject != null && object != null)
                result.add(new String[] {subject, predicate, object});

            // go deeper
            if ( item instanceof Map )
                parseMap( (Map) item, result, object);
        }
    }

    private String getObjectIdOrLiteralValue(Object object)
    {
        if (object instanceof String) // literal string
            return LITERAL_STRING_PREFIX + (String) object;

        else if (object instanceof Integer)
            return LITERAL_INTEGER_PREFIX + Integer.toString( (Integer) object );

        else if (object instanceof Float)
            return LITERAL_FLOAT_PREFIX + Float.toString( (Float) object );

        else if (object instanceof Double)
            return LITERAL_FLOAT_PREFIX + Double.toString( (Double) object );

        else if (object instanceof Boolean)
            return LITERAL_BOOLEAN_PREFIX + Boolean.toString( (Boolean) object );

        else if (object instanceof Map)
        {
            if ( ((Map) object).keySet().contains("@id"))
                return (String) ((Map) object).get("@id");
            else
                return newBlankNodeId();
        }

        // lists have no id.
        return null;
    }

    private String newBlankNodeId()
    {
        return BLANKNODE_PREFIX + (++m_lastBlankNodeId);
    }

    /*******************************************************************************************************************
     * Serialization (to flat JSON-LD)
     ******************************************************************************************************************/

    /**
     * Serialize a list of triples into a jsonld structure (which can be converted
     * to a jsonld string using an ObjectMapper).
     * @param triples must be a list of String[3] subject-predicate-object triples.
     */
    public static Map serialize(List<String[]> triples, Set<String> alwaysSets)
    {
        List graphList = new ArrayList<>();

        // an optimization to find items in the graph list without searching
        Map<String, Map> _optGraphMap = new HashMap<>();

        // Add all identifiable nodes first
        for (String[] triple : triples)
        {
            // Has this object already been added to the graph?
            if (_optGraphMap.keySet().contains(triple[0]))
            {
                Map objectMap = _optGraphMap.get(triple[0]);

                addTripleToObject(objectMap, triple, alwaysSets);
            }
            else
            {
                Map objectMap = new HashMap<>();
                objectMap.put("@id", triple[0]);
                graphList.add(objectMap);
                _optGraphMap.put(triple[0], objectMap);

                addTripleToObject(objectMap, triple, alwaysSets);
            }
        }

        Map result = new HashMap<>();
        result.put("@graph", graphList);

        return result;
    }

    private static void addTripleToObject(Map objectMap, String[] triple, Set<String> alwaysSets)
    {
        // The thing to be added might be just a string (if the triple object is a literal),
        // or a map containing an @id if the triple object is an identifiable node in itself.
        Object toBeAdded;
        if ( triple[2].startsWith(LITERAL_STRING_PREFIX) )
        {
            toBeAdded = triple[2].substring(LITERAL_STRING_PREFIX.length());
        }
        else if ( triple[2].startsWith(LITERAL_INTEGER_PREFIX) )
        {
            toBeAdded = Integer.parseInt( triple[2].substring(LITERAL_INTEGER_PREFIX.length()) );
        }
        else if ( triple[2].startsWith(LITERAL_FLOAT_PREFIX) )
        {
            toBeAdded = Double.parseDouble( triple[2].substring(LITERAL_FLOAT_PREFIX.length()) );
        }
        else if ( triple[2].startsWith(LITERAL_BOOLEAN_PREFIX) )
        {
            toBeAdded = Boolean.parseBoolean( triple[2].substring(LITERAL_BOOLEAN_PREFIX.length()) );
        }
        else
        {
            Map map = new HashMap();
            map.put("@id", triple[2]);
            toBeAdded = map;
        }

        // Does the object already have an instance of this predicate?
        // If so, convert that instance to a list
        if (objectMap.keySet().contains(triple[1]))
        {
            Object tmp = objectMap.get(triple[1]);
            List objectList = null;
            if (!(tmp instanceof List))
            {
                objectList = new ArrayList<>();
                objectMap.put(triple[1], objectList); // removal of the old object is implicit
                objectList.add(tmp);
            }
            else
                objectList = (List) tmp;

            objectList.add(toBeAdded);
        }
        else // add normally
        {
            // Is this predicate one of those that must always be in a list, even if there is just one of them?
            if ( alwaysSets.contains( triple[1] ) )
            {
                List objectList = new ArrayList<>();
                objectList.add(toBeAdded);
                objectMap.put(triple[1], objectList);
            }
            else
                objectMap.put(triple[1], toBeAdded);
        }
    }

    /*******************************************************************************************************************
     * Normalization (to the JSON-LD form kept in the Whelk database)
     ******************************************************************************************************************/

    /**
     * Assumes the provided jsonld to be flat.
     */
    public static void normalize(Map map, String mainId)
    {
        List graphList = (List) map.get("@graph");

        // find the resource (thing) node id. And the list indices for the main node and the thing node
        String thingId = null;
        int mainNodeIndex = -1;
        int thingNodeIndex = -1;
        for (int i = 0; i < graphList.size(); ++i)
        {
            Map objectMap = (Map) graphList.get(i);
            if (objectMap.containsKey("@id") &&  objectMap.get("@id").equals(Document.getBASE_URI()+mainId))
            {
                mainNodeIndex = i;
                Map thingReference = (Map) objectMap.get(JsonLd.getABOUT_KEY());
                thingId = (String) thingReference.get("@id");
            }
        }

        for (int i = 0; i < graphList.size(); ++i)
        {
            Map objectMap = (Map) graphList.get(i);
            if (objectMap.containsKey("@id") &&  objectMap.get("@id").equals(thingId))
                thingNodeIndex = i;
        }

        // Make sure the main node is at index 0 in the graph list and the thing node at index 1
        if (mainNodeIndex == 1 && thingNodeIndex == 0)
            Collections.swap(graphList, 0, 1);
        else if (mainNodeIndex != -1 && thingNodeIndex != -1)
        {
            if (mainNodeIndex != 0)
                Collections.swap(graphList, 0, mainNodeIndex);
            if (thingNodeIndex != 1)
                Collections.swap(graphList, 1, thingNodeIndex);
        }

        // Embed and delete root objects where possible
        Iterator it = graphList.iterator();
        while(it.hasNext())
        {
            Map objectMap = (Map) it.next();
            if (objectMap.containsKey("@id"))
            {
                String objectId = (String) objectMap.get("@id");

                if ( objectId.equals(mainId) || objectId.equals(thingId) )
                    continue;

                List references = new ArrayList<>();
                gatherReferences(map, objectId, references);

                if (references.size() > 0)
                {
                    for (Object reference: references)
                    {
                        Map referencingMap = (Map) reference;
                        referencingMap.putAll(objectMap);
                    }
                    it.remove();
                }
            }
        }

        pullQuoted(map);

        Set referencedBNodes = new HashSet();
        JsonLd.getReferencedBNodes(map, referencedBNodes);

        JsonLd.cleanUnreferencedBNodeIDs(map, referencedBNodes);
    }

    private static void gatherReferences(Map map, String id, List references)
    {
        if (map.size() == 1)
        {
            String key = (String) map.keySet().toArray()[0];
            if ( key.equals("@id") && map.get(key).equals(id) )
                references.add(map);
        }

        for (Object key : map.keySet())
        {
            Object subobject = map.get(key);

            if ( subobject instanceof Map )
                gatherReferences( (Map) subobject, id, references );
            else if ( subobject instanceof List )
                gatherReferences( (List) subobject, id, references );
        }
    }

    private static void gatherReferences(List list, String id, List references)
    {
        for (Object item : list)
        {
            if ( item instanceof Map )
                gatherReferences( (Map) item, id, references );
        }
    }

    /**
     * Pull all quoted data out of embedded objects and leave only references embedded
     */
    private static void pullQuoted(Map map)
    {
        List graphList = (List) map.get("@graph");

        for (Object object : graphList)
        {
            Map objectMap = (Map) object;
            if (objectMap.containsKey("@graph"))
            {
                Map quotedObject = (Map) objectMap.get("@graph");
                pullId(map, (String) quotedObject.get("@id"), quotedObject);
            }
        }
    }

    /**
     * Finds embedded objects with id 'id' and clears them out leaving only the id as a reference.
     * The object 'exception' is the only one allowed to keep the actual object in its entirety.
     */
    private static void pullId(Map map, String id, Object exception)
    {
        if ( map.containsKey("@id") && map.get("@id").equals(id) && map != exception )
        {
            map.clear();
            map.put("@id", id);
        }

        for (Object key : map.keySet())
        {
            Object subobject = map.get(key);

            if ( subobject instanceof Map )
                pullId( (Map) subobject, id, exception );
            else if ( subobject instanceof List )
                pullId( (List) subobject, id, exception );
        }
    }

    private static void pullId(List list, String id, Object exception)
    {
        for (Object item : list)
        {
            if ( item instanceof Map )
                pullId( (Map) item, id, exception );
        }
    }
}
