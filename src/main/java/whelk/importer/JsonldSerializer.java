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
     * Serialization (to flat JSON-LD)
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
            // Has this object already been added to the graph?
            if (_optGraphMap.keySet().contains(triple[0]))
            {
                Map objectMap = _optGraphMap.get(triple[0]);

                addTripleToObject(objectMap, triple);
            }
            else
            {
                Map objectMap = new HashMap<>();
                objectMap.put("@id", triple[0]);
                graphList.add(objectMap);
                _optGraphMap.put(triple[0], objectMap);

                addTripleToObject(objectMap, triple);
            }
        }

        // Half-frame to the form produced by the jsonld converter ?

        Map result = new HashMap<>();
        result.put("@graph", graphList);

        return result;
    }

    private static void addTripleToObject(Map objectMap, String[] triple)
    {
        // The thing to be added might be just a string (if the triple object is a literal),
        // or a map containing an @id if the triple object is an identifiable node in itself.
        Object toBeAdded;
        if ( triple[2].startsWith(LITERAL_PREFIX) )
        {
            toBeAdded = triple[2].substring(LITERAL_PREFIX.length());
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
            objectMap.put(triple[1], toBeAdded);
        }
    }

    /*******************************************************************************************************************
     * Normalization
     ******************************************************************************************************************/

    // method:
    // First find the main id, and its about id
    // Build jsonld
    // Embed everything that is not main or about id.
    // Place main id at 0 and about id at 1
    // clean out unnecessary bnodeids

    /**
     * Assumes the provided jsonld to be flat.
     */
    public static void normalize(Map map, String mainId)
    {
        List graphList = (List) map.get("@graph");

        // find the resource (thing) node id
        String thingId = null;
        for (Object object : graphList)
        {
            Map objectMap = (Map) object;
            if (objectMap.containsKey("@id") &&  objectMap.get("@id").equals(Document.getBASE_URI()+mainId))
            {
                Map thingReference = (Map) objectMap.get(Document.getABOUT_KEY());
                thingId = (String) thingReference.get("@id");
            }
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
}
