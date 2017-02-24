package whelk

import groovy.util.logging.Slf4j as Log
import org.apache.lucene.analysis.util.CharArrayMap
import org.codehaus.jackson.map.ObjectMapper
import whelk.util.PropertyLoader

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

    // If we _statically_ call loadProperties("secret"), without a try/catch it means that no code with a dependency on
    // whelk-core can ever run without a secret.properties file, which for example unit tests (for other projects
    // depending on whelk-core) sometimes need to do.
    static final URI BASE_URI

    static
    {
        try {
            BASE_URI = new URI(PropertyLoader.loadProperties("secret").get("baseUri", "https://libris.kb.se/"))
        }
        catch (Exception e) {
            System.err.println(e)
            BASE_URI = new URI("https://libris.kb.se/");
        }
    }

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
    private boolean deleted = false


    Document(Map data) {
        this.data = data
    }

    Document clone() {
        Map clonedDate = deepCopy(data)
        return new Document(clonedDate)
    }

    URI getURI() {
        return BASE_URI.resolve(getShortId())
    }

    String getDataAsString() {
        return mapper.writeValueAsString(data)
    }

    void setApixExportFailFlag(boolean failed) { set(failedApixExportPath, failed, LinkedHashMap) }

    boolean getApixExportFailFlag() { get(failedApixExportPath) }

    void setControlNumber(controlNumber) { set(controlNumberPath, controlNumber, LinkedHashMap) }

    String getControlNumber() { get(controlNumberPath) }

    void setHoldingFor(holdingFor) { set(holdingForPath, holdingFor, LinkedHashMap) }

    String getHoldingFor() { get(holdingForPath) }

    void setEncodingLevel(encLevel) { set(encLevelPath, encLevel, LinkedHashMap) }

    String getEncodingLevel() { get(encLevelPath) }

    /**
     * Will have base URI prepended if not already there
     */
    void setId(String id) {
        if (!id.startsWith(Document.BASE_URI.toString()))
            id = Document.BASE_URI.resolve(id)

        addRecordIdentifier(id)
    }

    /**
     * Gets the document id (short form, without base URI).
     */
    String getShortId() {
        String base = Document.BASE_URI.toString()
        for (id in getRecordIdentifiers())
            if (id.startsWith(base))
                return id.substring(base.length())
        return null
    }

    /**
     * Gets the document id (long form with base uri)
     */
    String getCompleteId() {
        return get(recordIdPath)
    }

    /**
     * Alias for getCompleteId, for backwards compatibility.
     */
    String getId() { return getCompleteId() }

    void setCreated(Date created) {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(created.toInstant(), ZoneId.systemDefault())
        String formatedCreated = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt)
        set(createdPath, formatedCreated, HashMap)
    }

    String getCreated() {
        get(createdPath)
    }

    void setModified(Date modified) {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(modified.toInstant(), ZoneId.systemDefault())
        String formatedModified = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt)
        set(modifiedPath, formatedModified, HashMap)
    }

    String getModified() { get(modifiedPath) }

    void setDeleted(boolean newValue) {
        deleted = newValue
    }

    boolean getDeleted() {
        return deleted
    }

    boolean isHolding() {
        List graphList = this.data.get("@graph")
        return graphList.any { it ->
            it.get('@type')?.equalsIgnoreCase('Item')
        }
    }

    String getSigel() {
        if (this.isHolding()) {
            List graphItems = this.data.get("@graph")

            Map item = graphItems.find { element ->
                element[JsonLd.TYPE_KEY] == 'Item'
            }

            if (item.heldBy?.notation) {
                return item.heldBy.notation
            } else {
                return null
            }
        } else {
            // TODO undefined for non-holding posts for now
            return null
        }
    }


    /**
     * By convention the first id in the returned list is the MAIN resource id.
     */
    List<String> getThingIdentifiers() {
        List<String> ret = []

        String thingId = get(thingIdPath)
        if (thingId)
            ret.add(thingId)

        List sameAsObjects = get(thingSameAsPath)
        for (Map object : sameAsObjects) {
            ret.add(object.get("@id"))
        }

        return ret
    }

    void addThingIdentifier(String identifier) {
        if (get(thingIdPath) == null) {
            set(thingIdPath2, identifier, HashMap)
            set(thingIdPath, identifier, HashMap)
            return
        }

        if (preparePath(thingSameAsPath, ArrayList)) {
            List sameAsList = get(thingSameAsPath)
            def idObject = ["@id": identifier]
            if (!sameAsList.contains(idObject))
                sameAsList.add(idObject)
        }
    }

    /**
     * By convention the first id in the returned list is the MAIN record id.
     */
    List<String> getRecordIdentifiers() {
        List<String> ret = []

        ret.add(get(recordIdPath)) // must be present

        List sameAsObjects = get(recordSameAsPath)
        for (Map object : sameAsObjects) {
            if (object.get("@id") != null)
                ret.add(object.get("@id"))
        }

        return ret
    }

    void addRecordIdentifier(String identifier) {
        if (identifier == null)
            throw new NullPointerException("Attempted to add null-identifier.")

        if (get(recordIdPath) == null) {
            set(recordIdPath, identifier, HashMap)
            return
        }

        if (get(recordIdPath) == identifier) {
            return
        }

        if (preparePath(recordSameAsPath, ArrayList)) {
            Object sameAsList = get(recordSameAsPath)
            def idObject = ["@id": identifier]
            if (sameAsList.every { it -> it != idObject })
                sameAsList.add(idObject)
        }
    }

    /**
     * Expand the doc with the supplied extra info.
     *
     */
    void embellish(Map additionalObjects, Map displayData) {
        this.data = JsonLd.embellish(this.data, additionalObjects, displayData)
        return
    }

    /**
     * Return a list of external references in the doc.
     *
     */
    List getExternalRefs() {
        return JsonLd.getExternalReferences(this.data)
    }

    /**
     * Convert a list of posts to cards.
     *
     */
    List toCards(List things, Map displayData) {
        return JsonLd.toCards(things, displayData)
    }

    /**
     * Convert a post to card.
     *
     */
    void toCard(Map displayData) {
        this.data = JsonLd.toCard(this.data, displayData)
        return
    }

    /**
     * Convert a list of posts to chips.
     *
     */
    List toChips(List things, Map displayData) {
        return JsonLd.toChips(things, displayData)
    }

    /**
     * Convert a post to chip.
     *
     */
    Map toChip(Map displayData) {
        return this.data = JsonLd.toChip(this.data, displayData)
    }



    /**
     * Adds empty structure to the document so that 'path' can be traversed.
     */
    private boolean preparePath(List path, Type leafType) {
        // Start at root data node
        Object node = data

        for (int i = 0; i < path.size(); ++i) {
            Object step = path.get(i)

            Type nextReplacementType
            if (i < path.size() - 1) // use the next step to determine the type of the next object
                nextReplacementType = (path.get(i + 1) instanceof Integer) ? ArrayList : HashMap
            else
                nextReplacementType = leafType

            // Get the next object along the path (candidate)
            Object candidate = null
            if (node instanceof Map)
                candidate = node.get(step)
            else if (node instanceof List) {
                if (node.size() > step)
                    candidate = node.get(step)
            }

            // If that step can't be taken in the current structure, expand the structure
            if (candidate == null) {
                if (node instanceof Map)
                    node.put(step, nextReplacementType.newInstance())
                else if (node instanceof List) {
                    while (node.size() < step + 1)
                        node.add(nextReplacementType.newInstance())
                }
            }
            // Check path integrity, in all but the last step (which will presumably be replaced)
            else if ((i < path.size() - 1) &&
                    !matchingContainer(candidate.getClass(), nextReplacementType)) {
                log.warn("Structure conflict, path: " + path + ", at token: " + (i + 1) + ", expected data to be: " + nextReplacementType + ", data was: " + candidate.getClass() + ", data:\n" + data)
                return false
            }

            node = node.get(step)
        }
        return true
    }

    /**
     * Set 'value' at 'path'. 'container' should be ArrayList or HashMap depending on if value should reside in a list or an object
     */
    private boolean set(List path, Object value, Type container) {
        if (!preparePath(path, container))
            return false

        // Start at root data node
        Object node = data

        for (int i = 0; i < path.size() - 1; ++i) // follow all but last step
        {
            Object step = path.get(i)
            node = node.get(step)
        }

        node.put(path.get(path.size() - 1), value)
        return true
    }

    private boolean matchingContainer(Class c1, Class c2) {
        if ((Map.class.isAssignableFrom(c1)) && (Map.class.isAssignableFrom(c2)))
            return true
        if ((List.class.isAssignableFrom(c1)) && (List.class.isAssignableFrom(c2)))
            return true
        return false
    }

    private Object get(List path) {
        // Start at root data node
        Object node = data

        for (Object step : path) {
            if ((node instanceof Map) && !(step instanceof String)) {
                log.warn("Needed string as map key, but was given: " + step + ". (path was: " + path + ")")
                return null
            } else if ((node instanceof List) && !(step instanceof Integer)) {
                log.warn("Needed integer as list index, but was given: " + step + ". (path was: " + path + ")")
                return null
            }
            node = node[step]

            if (node == null) {
                return null
            }
        }

        return node
    }

    static Object deepCopy(Object orig) {
        //TODO: see https://jira.kb.se/browse/LXL-270
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream()
            ObjectOutputStream oos = new ObjectOutputStream(bos)
            oos.writeObject(orig); oos.flush()
            ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray())
            ObjectInputStream ois = new ObjectInputStream(bin)
            return ois.readObject()
        } catch (any) {
            //§println "ERROR! ${any.message} in deepCopy. Cloning using other approach"
            def o = orig.inspect()
            ByteArrayOutputStream bos = new ByteArrayOutputStream()
            ObjectOutputStream oos = new ObjectOutputStream(bos)
            oos.writeObject(o); oos.flush()
            ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray())
            ObjectInputStream ois = new ObjectInputStream(bin)
            return Eval.me(ois.readObject())
        }
    }

    /**
     * This function relies on the fact that the deserialized jsonld (using Jackson ObjectMapper) consists of LinkedHashMaps
     * (which preserve order), unlike normal HashMaps which do not, so be careful not to place HashMaps into a document
     * structure and then try to calculate a checksum. Makes the order of the inner elements not matter for the caclulation
     *
     * This method of getting checksums is deprecated, because it is extremely expensive, and comparing full documents
     * would actually be faster than this.
     */
    /*
    String getChecksum() {
        Document clone = clone()

        // timestamps not part of checksum
        clone.set(modifiedPath, "", LinkedHashMap)
        clone.set(createdPath, "", LinkedHashMap)
        clone.data = clone.data["@graph"].collectEntries {
            it ->
                it.toSorted { a, b -> a.key <=> b.key }
        }
        MessageDigest m = MessageDigest.getInstance("MD5")
        m.reset()
        byte[] databytes = mapper.writeValueAsBytes(clone.data)
        m.update(databytes)
        byte[] digest = m.digest()
        BigInteger bigInt = new BigInteger(1, digest)
        String hashtext = bigInt.toString(16)
        return hashtext
    }*/

    String getChecksum() {
        long checksum = calculateCheckSum(data, 0)
        return Long.toString(checksum)
    }

    private long calculateCheckSum(node, int depth) {
        long term = 0

        if (node == null)
            return term
        else if (node instanceof String)
            return node.hashCode() + depth
        else if (node instanceof Boolean)
            return node.booleanValue() ? 1 + depth : depth
        else if (node instanceof Integer)
            return node.intValue() + depth
        else if (node instanceof Map) {
            for (String key : node.keySet()) {
                if ( !key.equals(JsonLd.MODIFIED_KEY) && !key.equals(JsonLd.CREATED_KEY)) {
                    term += key.hashCode() + depth
                    term += calculateCheckSum(node[key], depth + 1)
                }
            }
        }
        else {
            for (entry in node)
                term += calculateCheckSum(entry, depth+1)
        }

        return term
    }
}
