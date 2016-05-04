package whelk.importer;

import java.util.*;

/**
 * This class serializes and deserializes RDF triples (represented as String[3]) to/from a subset of jsonld.
 */
public class JsonldSerializer
{
    private static int lastBlankNodeId = -1;
    public static final String LITERAL_PREFIX = "__literal:";
    public static final String BLANKNODE_PREFIX = "_:b";

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
    public static List<String[]> deserialize(Map jsonMap)
    {
        List<String[]> result = new ArrayList<String[]>();
        parseMap(jsonMap, result, null);
        return result;
    }

    /**
     * nodeId is the id of the node we're currently traversing, which might be a blank node id
     * (or null at the beginning).
     */
    private static void parseMap(Map jsonMap, List<String[]> result, String nodeId)
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
                object = getObjectId(objectCandidate);

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

    private static void parseList(List jsonList, List<String[]> result, String nodeId, String predicate)
    {
        for (Object item : jsonList)
        {
            String subject = nodeId;
            String object = getObjectId(item);

            // store triple
            if (subject != null && object != null)
                result.add(new String[] {subject, predicate, object});

            // go deeper
            if ( item instanceof Map )
                parseMap( (Map) item, result, object);
        }
    }

    private static String getObjectId(Object object)
    {
        if (object instanceof String) // literal object
            return LITERAL_PREFIX + (String) object;

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

    private static String newBlankNodeId()
    {
        return BLANKNODE_PREFIX + ++lastBlankNodeId;
    }

    /*******************************************************************************************************************
     * Serialization
     ******************************************************************************************************************/

    /**
     * Serialize a list of triples into a jsonld structure (which can be converted
     * to a jsonld string using an ObjectMapper).
     * @param triples must be a list of String[3] subject-predicate-object triples.
     */
    public static Map serialize(List<String[]> triples)
    {
        List graphList = new ArrayList<>();

        // an optimization to find items in the graph list without searching
        Map<String, Map> _optGraphMap = new HashMap<>();

        // Add all identifiable nodes first
        for (String[] triple : triples)
        {
            if ( !triple[0].startsWith(BLANKNODE_PREFIX) )
            {
                // Has this object already been added to the graph?
                if (_optGraphMap.keySet().contains(triple[0]))
                {
                    Map objectMap = _optGraphMap.get(triple[0]);

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

                        objectList.add(triple[2]);
                    }
                    else // add normally
                    {
                        objectMap.put(triple[1], triple[2]);
                    }
                }
                else
                {
                    Map objectMap = new HashMap<>();
                    objectMap.put("@id", triple[0]);
                    objectMap.put(triple[1], triple[2]);
                    graphList.add(objectMap);
                    _optGraphMap.put(triple[0], objectMap);
                }
            }
        }

        // Embed all blank nodes

        Map result = new HashMap<>();
        result.put("@graph", graphList);
        return result;
    }

}
