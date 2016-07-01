package whelk

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.*

import java.lang.reflect.Array
import java.lang.reflect.Type
import java.security.MessageDigest
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/**
 * A document is represented as a data Map (containing Maps, Lists and Value objects).
 *
 * This class serves as a wrapper around such a map, with access methods for specific parts of the data.
 */
@Log
class Document {
    static final ObjectMapper mapper = new ObjectMapper()

    static final List thingIdPath = ["@graph", 0, "mainEntity", "@id"]
    static final List thingIdPath2 = ["@graph", 1, "@id"]
    static final List thingSameAsPath = ["@graph", 1, "sameAs"]
    static final List recordIdPath = ["@graph", 0, "@id"]
    static final List recordSameAsPath = ["@graph", 0, "sameAs"]
    static final List failedApixExportPath = ["@graph", 0, "apixExportFailedAt"]
    static final List controlNumberPath = ["@graph", 0, "controlNumber"]
    static final List holdingForPath = ["@graph", 1, "holdingFor", "@id"]
    static final List createdPath = ["@graph", 0, "created"]
    static final List modifiedPath = ["@graph", 0, "modified"]
    static final List encLevelPath = ["@graph", 0, "marc:encLevel", "@id"]

    public Map data = [:]

    Document(Map data) {
        this.data = data;
    }

    Document clone()
    {
        Map clonedDate = deepCopy(data)
        return new Document(clonedDate)
    }

    URI getURI()
    {
        return JsonLd.BASE_URI.resolve(getId())
    }

    String getDataAsString()
    {
        return mapper.writeValueAsString(data)
    }

    void setId(id) { set(recordIdPath, id, HashMap) }
    String getId() { get(recordIdPath) }

    void setApixExportFailFlag(boolean failed) { set(failedApixExportPath, failed, HashMap) }
    boolean getApixExportFailFlag() { get(failedApixExportPath) }

    void setControlNumber(controlNumber) { set(controlNumberPath, controlNumber, HashMap) }
    String getControlNumber() { get(controlNumberPath) }

    void setHoldingFor(holdingFor) { set(holdingForPath, holdingFor, HashMap) }
    String getHoldingFor() { get(holdingForPath) }

    void setEncodingLevel(encLevel) { set(encLevelPath, encLevel, HashMap) }
    String getEncodingLevel() { get(encLevelPath) }

    void setCreated(Date created)
    {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(created.toInstant(), ZoneId.systemDefault())
        String formatedCreated = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt)
        set(createdPath, formatedCreated, HashMap)
    }
    String getCreated() { get(createdPath) }

    void setModified(Date modified)
    {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(modified.toInstant(), ZoneId.systemDefault())
        String formatedModified = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt)
        set(modifiedPath, formatedModified, HashMap)
    }
    String getModified() { get(modifiedPath) }

    /**
     * By convention the first id in the returned list is the MAIN resource id.
     */
    List<String> getThingIdentifiers()
    {
        List<String> ret = []

        ret.add( get(thingIdPath) ) // must come first in the list.

        List sameAsObjects = get(thingSameAsPath)
        for (Map object : sameAsObjects)
        {
            ret.add( object.get("@id") )
        }

        return ret
    }

    void addThingIdentifier(String identifier)
    {
        if (get(thingIdPath) == null)
        {
            set(thingIdPath2, identifier, HashMap)
            set(thingIdPath, identifier, HashMap)
            return
        }

        if (preparePath(thingSameAsPath, ArrayList))
        {
            List sameAsList = get(thingSameAsPath)
            sameAsList.add(["@id" : identifier])
        }
    }

    /**
     * By convention the first id in the returned list is the MAIN record id.
     */
    List<String> getRecordIdentifiers()
    {
        List<String> ret = []

        ret.add( get(recordIdPath) ) // must come first in the list.

        List sameAsObjects = get(recordSameAsPath)
        for (Map object : sameAsObjects)
        {
            ret.add( object.get("@id") )
        }

        return ret
    }

    void addRecordIdentifier(String identifier)
    {
        if (get(recordIdPath) == null)
        {
            set(recordIdPath, identifier, HashMap)
            return
        }

        if (preparePath(recordSameAsPath, ArrayList))
        {
            List sameAsList = get(recordSameAsPath)
            sameAsList.add(["@id" : identifier])
        }
    }

    /**
     * Adds empty structure to the document so that 'path' can be traversed.
     */
    private boolean preparePath(List path, Type leafType)
    {
        // Start at root data node
        Object node = data;

        for (int i = 0; i < path.size(); ++i)
        {
            Object step = path.get(i)

            Type nextReplacementType;
            if (i < path.size() - 1) // use the next step to determine the type of the next object
                nextReplacementType = (path.get(i+1) instanceof Integer) ? ArrayList : HashMap
            else
                nextReplacementType = leafType

            // Get the next object along the path (candidate)
            Object candidate = null;
            if (node instanceof Map)
                candidate = node.get(step)
            else if (node instanceof List)
            {
                if (node.size() > step)
                    candidate = node.get(step)
            }

            // If that step can't be taken in the current structure, expand the structure
            if (candidate == null)
            {
                if (node instanceof Map)
                    node.put(step, nextReplacementType.newInstance())
                else if (node instanceof List)
                {
                    while (node.size() < step+1)
                        node.add(nextReplacementType.newInstance())
                }
            }
            // Check path integrity, in all but the last step (which will presumably be replaced)
            else if ((i < path.size() - 1) &&
                    ! matchingContainer(candidate.getClass(), nextReplacementType))
            {
                log.warn("Structure conflict, path: " + path + ", at token: " + (i+1) + ", expected data to be: " + nextReplacementType + ", data was: " + candidate.getClass() + ", data:\n" + data );
                return false
            }

            node = node.get(step)
        }
        return true
    }

    /**
     * Set 'value' at 'path'. 'container' should be ArrayList or HashMap depending on if value should reside in a list or an object
     */
    private boolean set(List path, Object value, Type container)
    {
        if (! preparePath(path, container))
            return false

        // Start at root data node
        Object node = data;

        for (int i = 0; i < path.size() - 1; ++i) // follow all but last step
        {
            Object step = path.get(i)
            node = node.get(step)
        }

        node.put(path.get(path.size()-1), value)
        return true;
    }

    private boolean matchingContainer(Class c1, Class c2)
    {
        if ( (Map.class.isAssignableFrom(c1)) && (Map.class.isAssignableFrom(c2)) )
            return true
        if ( (List.class.isAssignableFrom(c1)) && (List.class.isAssignableFrom(c2)) )
            return true
        return false;
    }

    private Object get(List path)
    {
        // Start at root data node
        Object node = data;

        for (Object step : path)
        {
            if ( (node instanceof Map) && !(step instanceof String) )
            {
                log.warn("Needed string as map key, but was given: " + step + ". (path was: " + path + ")")
                return null;
            }
            else if ( (node instanceof List) && !(step instanceof Integer) )
            {
                log.warn("Needed integer as list index, but was given: " + step + ". (path was: " + path + ")")
                return null;
            }
            node = node.get(step)

            if (node == null)
                return null;
        }

        return node;
    }

    static Object deepCopy(Object orig)
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        ObjectOutputStream oos = new ObjectOutputStream(bos)
        oos.writeObject(orig); oos.flush()
        ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray())
        ObjectInputStream ois = new ObjectInputStream(bin)
        return ois.readObject()
    }

    String getChecksum()
    {
        Document clone = clone();

        // timestamps not part of checksum
        clone.set(modifiedPath, "", HashMap)
        clone.set(createdPath, "", HashMap)

        MessageDigest m = MessageDigest.getInstance("MD5")
        m.reset()
        byte[] databytes = mapper.writeValueAsBytes(clone.data)
        m.update(databytes)
        byte[] digest = m.digest()
        BigInteger bigInt = new BigInteger(1,digest)
        String hashtext = bigInt.toString(16)
        return hashtext
    }
}
