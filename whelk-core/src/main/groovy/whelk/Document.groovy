package whelk

import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

import java.lang.reflect.Type
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
    static URI BASE_URI

    static
    {
        try {
            BASE_URI = new URI( (String) PropertyLoader.loadProperties("secret").get("baseUri", "https://libris.kb.se/") )
        }
        catch (Exception e) {
            System.err.println(e)
            BASE_URI = new URI("https://libris.kb.se/")
        }
    }

    static final ObjectMapper mapper = new ObjectMapper()

    static final List thingPath = ["@graph", 1]
    static final List thingIdPath = ["@graph", 0, "mainEntity", "@id"]
    static final List thingIdPath2 = ["@graph", 1, "@id"]
    static final List thingTypePath = ["@graph", 1, "@type"]
    static final List thingSameAsPath = ["@graph", 1, "sameAs"]
    static final List thingTypedIDsPath = ["@graph", 1, "identifiedBy"]
    static final List thingIndirectTypedIDsPath = ["@graph", 1, "indirectlyIdentifiedBy"]
    static final List thingCarrierTypesPath = ["@graph", 1, "carrierType"]
    static final List recordIdPath = ["@graph", 0, "@id"]
    static final List workIdPath = ["@graph", 1, "instanceOf", "@id"]
    static final List thingMetaPath = ["@graph", 1, "meta", "@id"]
    static final List recordSameAsPath = ["@graph", 0, "sameAs"]
    static final List recordTypedIDsPath = ["@graph", 0, "identifiedBy"]
    static final List controlNumberPath = ["@graph", 0, "controlNumber"]
    static final List holdingForPath = ["@graph", 1, "itemOf", "@id"]
    static final List heldByPath = ["@graph", 1, "heldBy", "@id"]
    static final List createdPath = ["@graph", 0, "created"]
    static final List modifiedPath = ["@graph", 0, "modified"]
    static final List encLevelPath = ["@graph", 0, "encodingLevel"]
    static final List statusPath = ["@graph", 0, "recordStatus"]
    static final List sigelPath = ["@graph", 1, "heldBy", "@id"]
    static final List generationProcessPath = ["@graph", 0, "generationProcess", "@id"]
    static final List generationDatePath = ["@graph", 0, "generationDate"]
    static final List descriptionCreatorPath = ["@graph", 0, "descriptionCreator", "@id"]
    static final List descriptionLastModifierPath = ["@graph", 0, "descriptionLastModifier", "@id"]

    URI baseUri = BASE_URI

    public Map data = [:]
    public int version = 0

    Document(Map data) {
        this.data = data
        updateRecordStatus()
    }

    Document clone() {
        Map clonedDate = deepCopy(data)
        return new Document(clonedDate)
    }

    URI getURI() {
        return baseUri.resolve(getShortId())
    }

    String getDataAsString() {
        return mapper.writeValueAsString(data)
    }

    void setControlNumber(controlNumber) { set(controlNumberPath, controlNumber) }

    String getControlNumber() { get(controlNumberPath) }

    void setGenerationProcess(process) { set(generationProcessPath, process) }

    String getGenerationProcess() { get(generationProcessPath) }

    void setHoldingFor(holdingFor) { set(holdingForPath, holdingFor) }

    String getHoldingFor() { get(holdingForPath) }

    String getHeldBy() { get(heldByPath) }

    void setEncodingLevel(encLevel) { set(encLevelPath, encLevel) }

    String getEncodingLevel() { get(encLevelPath) }

    void setDescriptionCreator(creator) { set(descriptionCreatorPath, creator) }

    String getDescriptionCreator() { get(descriptionCreatorPath) }

    void setDescriptionLastModifier(modifier) { set(descriptionLastModifierPath, modifier) }

    String getDescriptionLastModifier() { get(descriptionLastModifierPath) }

    void setThingType(thingType) { set(thingTypePath, thingType) }

    String getThingType() { get(thingTypePath) }

    String getRecordStatus() { return get(statusPath) }

    void setRecordStatus(status) { set(statusPath, status) }

    void setThingMeta(meta) { set(thingMetaPath, meta) }

    /**
     * Will have base URI prepended if not already there
     */
    void setId(String id) {
        if (!id.startsWith(baseUri.toString()))
            id = baseUri.resolve(id)

        set(recordIdPath, id)
    }

    /**
     * Gets the document id (short form, without base URI).
     */
    String getShortId() {
        String base = baseUri.toString()
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
     * Gets the document system ID.
     * Usually this is equivalent to getComleteId(). But getComleteId() can sometimes return pretty-IDs
     * (like https://id.kb.se/something) when appropriate. This function will always return the complete
     * system internal ID (like so: [baseUri]/fnrglbrgl)
     */
    String getCompleteSystemId() {
        String shortId = getShortId()
        if (shortId != null)
            return baseUri.toString() + shortId
        return null
    }

    /**
     * Alias for getCompleteId, for backwards compatibility.
     */
    String getId() { return getCompleteId() }

    List<String> getIsbnValues() { return getTypedIDValues("ISBN", thingTypedIDsPath, "value") }
    List<String> getIssnValues() { return getTypedIDValues("ISSN", thingTypedIDsPath, "value") }
    List<String> getIsbnHiddenValues() { return getTypedIDValues("ISBN", thingIndirectTypedIDsPath, "value") }
    List<String> getIssnHiddenValues() { return getTypedIDValues("ISSN", thingTypedIDsPath, "marc:canceledIssn") }

    private List<String> getTypedIDValues(String typeKey, List<String> idListPath, String valueKey) {
        List<String> values = new ArrayList<>()
        List typedIDs = get(idListPath)
        for (Object element : typedIDs) {
            if (!(element instanceof Map))
                continue
            Map map = (Map) element

            Object type = map.get("@type")
            if (type == null)
                continue

            if (!type.equals(typeKey))
                continue

            Object value = map.get(valueKey)
            if (value != null) {
                if (value instanceof List)
                    for (Object object : value)
                        values.add( (String) object)
                else
                    values.add( (String) value)
            }
        }
        return values
    }

    List<Map> getCarrierTypes() {
        return get(thingCarrierTypesPath)
    }

    void setCreated(Date created) {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(created.toInstant(), ZoneId.systemDefault())
        String formatedCreated = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt)
        set(createdPath, formatedCreated)
        updateRecordStatus()
    }

    String getCreated() {
        get(createdPath)
    }

    void setModified(Date modified) {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(modified.toInstant(), ZoneId.systemDefault())
        String formatedModified = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt)
        set(modifiedPath, formatedModified)
        updateRecordStatus()
    }

    String getModified() { get(modifiedPath) }

    void setGenerationDate(Date generationDate) {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(generationDate.toInstant(), ZoneId.systemDefault())
        String formatedGenerationDate = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt)
        set(generationDatePath, formatedGenerationDate)
        updateRecordStatus()
    }

    String getGenerationDate() { get(generationDatePath) }

    void setDeleted(boolean newValue) {
        if (newValue)
            set(statusPath, "marc:Deleted")
        else {
            set(statusPath, "marc:New")
            updateRecordStatus()
        }
    }

    boolean getDeleted() {
        String deletedString = get(statusPath)
        if (deletedString == null || !deletedString.equals("marc:Deleted"))
            return false
        return true
    }

    private void updateRecordStatus() {
        String currentStatus = get(statusPath)
        if (currentStatus != null && currentStatus.equals("marc:New")) {
            String modified = getModified()
            String created = getCreated()
            if (modified != null && created != null && !modified.equals(created))
                set(statusPath, "marc:CorrectedOrRevised")
        }
    }

    boolean isHolding(JsonLd jsonld) {
        return ("hold" == LegacyIntegrationTools.determineLegacyCollection(this, jsonld))
    }

    String getSigel() {
        String uri = get(sigelPath)
        if (uri != null)
            return LegacyIntegrationTools.uriToLegacySigel( uri )
        return null
    }

    /**
     * Gets a list of [type, value, graphIndex] typed identifiers for this thing (mainEntity)
     */
    List<Tuple> getTypedThingIdentifiers() {
        List<Tuple> results = []
        List typedIDs = get(thingTypedIDsPath)

        for (Map typedID : typedIDs) {
            String type = typedID["@type"]
            String value = null
            if (typedID["value"] instanceof List)
                value = typedID["value"][0]
            else
                value = typedID["value"]

            if (type != null && value != null)
                results.add(new Tuple(type, value, 1))
        }

        return results
    }

    /**
     * Gets a list of [type, value, graphIndex] typed identifiers for this record
     */
    List<Tuple> getTypedRecordIdentifiers() {
        List<Tuple> results = []
        List typedIDs = get(recordTypedIDsPath)

        for (Map typedID : typedIDs) {
            String type = typedID["@type"]
            String value = null
            if (typedID["value"] instanceof List)
                value = typedID["value"][0]
            else
                value = typedID["value"]

            if (type != null && value != null)
                results.add(new Tuple(type, value, 0))
        }

        return results
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
            set(thingIdPath2, identifier)
            set(thingIdPath, identifier)
            return
        }

        if (get(thingIdPath) == identifier && get(thingIdPath2) == identifier) {
            return
        }

        if (preparePath(thingSameAsPath)) {
            List sameAsList = get(thingSameAsPath)
            if (sameAsList == null || !(sameAsList instanceof List)) {
                set(thingSameAsPath, [])
                sameAsList = get(thingSameAsPath)
            }
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

        String mainId = get(recordIdPath)
        if (mainId != null)
        ret.add(mainId)

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
            set(recordIdPath, identifier)
            return
        }

        if (get(recordIdPath) == identifier) {
            return
        }

        if (preparePath(recordSameAsPath)) {
            Object sameAsList = get(recordSameAsPath)
            if (sameAsList == null || !(sameAsList instanceof List)) {
                set(recordSameAsPath, [])
                sameAsList = get(recordSameAsPath)
            }
            def idObject = ["@id": identifier]
            if (sameAsList.every { it -> it != idObject })
                sameAsList.add(idObject)
        }
    }

    void addTypedThingIdentifier(String type, String identifier) {
        addTypedIdentifier(type, identifier, thingTypedIDsPath)
    }

    void addIndirectTypedThingIdentifier(String type, String identifier) {
        addTypedIdentifier(type, identifier, thingIndirectTypedIDsPath)
    }

    void addTypedRecordIdentifier(String type, String identifier) {
        addTypedIdentifier(type, identifier, recordIdPath)
    }

    private void addTypedIdentifier(String type, String identifier, List<Serializable> typedIdsPath) {
        if (identifier == null)
            throw new NullPointerException("Attempted to add typed null-identifier.")

        if (preparePath(typedIdsPath)) {
            Object typedIDList = get(typedIdsPath)
            if (typedIDList == null || !(typedIDList instanceof List)) {
                set(typedIdsPath, [])
                typedIDList = get(typedIdsPath)
            }

            def idObject = ["value": identifier, "@type": type]
            if (typedIDList.every { it -> it != idObject })
                typedIDList.add(idObject)
        }
    }

    public String getWorkType() {
        Object workId = get(workIdPath)
        if (workId == null)
            return null

        Map workObject = getEmbedded( (String) workId )
        if (workObject == null)
            return null

        String type = workObject.get("@type")
        if (type != null)
            return type
        return null
    }

    /**
     * Get the embedded/embellished object for a certain id.
     * Will return null if there is no object with the requested id in this documents data.
     */
    private Map getEmbedded(String id) {
        List graphList = (List) data.get("@graph")
        if (graphList == null)
            return null

        return getEmbedded(id, graphList)
    }

    private Map getEmbedded(String id, Map localData) {
        Map map = (Map) localData
        String objId = map.get("@id")
        if (objId.equals(id) && map.size() > 1) {
            return map
        }
        else {
            for (Object key : localData.keySet()) {
                Object object = localData.get(key)
                if ((object instanceof List)) {
                    Map next = getEmbedded(id, (List) object)
                    if (next != null)
                        return next
                }
                else if ((object instanceof Map)) {
                    Map next = getEmbedded(id, (Map) object)
                    if (next != null)
                        return next
                }
                else
                    return null
            }
        }
        return null
    }

    private Map getEmbedded(String id, List localData) {
        for (Object object : localData) {
            Map map = (Map) object
            Map candidate = getEmbedded(id, map)
            if (candidate != null)
                return candidate
        }
        return null
    }

    /**
     * Return a list of external references in the doc.
     *
     */
    List getExternalRefs() {
        return JsonLd.getExternalReferences(this.data)
    }

    /**
     * Returns a String[2], first of which is the relation (itemOf, heldBy etc).
     * The second string is the referenced URI.
     */
    List<String[]> getRefsWithRelation() {
        List<String[]> references = new ArrayList<>()

        addRefsWithRelation(this.data, references, null)

        return references
    }

    private void addRefsWithRelation(Map node, List<String[]> references, String relation) {
        for (Object keyObject : node.keySet()) {
            String key = (String) keyObject
            Object value = node.get(keyObject)

            // Is node a {"@id":"[URI]"} link ?
            if (key.equals("@id") && node.size() == 1 && value instanceof String) {
                references.add ([relation, (String) value] as String[])
            }

            // Keep going down the tree
            if (value instanceof Map)
                addRefsWithRelation( (Map) value, references, key )
            else if ( value instanceof List)
                addRefsWithRelation( (List) value, references, key )
        }
    }

    private void addRefsWithRelation(List node, List<String[]> references, String relation) {
        // Keep going down the tree
        for (Object item : node) {
            if (item instanceof Map)
                addRefsWithRelation( (Map) item, references, relation )
            else if ( item instanceof List)
                addRefsWithRelation( (List) item, references, relation )
        }
    }

    /**
     * Adds empty structure to the document so that 'path' can be traversed.
     */
    private boolean preparePath(List path) {
        return _preparePath(path, data)
    }

    public static boolean _preparePath(List path, Object root) {
        // Start at root data node
        Object node = root

        for (int i = 0; i < path.size()-1; ++i) {
            Object step = path.get(i)

            // use the next step to determine the type of the next object
            Type nextReplacementType = (path.get(i + 1) instanceof Integer) ? ArrayList : HashMap

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
                log.warn("Structure conflict, path: " + path + ", at token: " +
                         (i + 1) + ", expected data to be: " +
                         nextReplacementType + ", data class was: " +
                         candidate.getClass())
                log.debug("preparePath integrity check failed, data was: ${root}")
                return false
            }

            node = node.get(step)
        }
        return true
    }

    /**
     * Set 'value' at 'path'. 'container' should be ArrayList or HashMap depending on if value should reside in a list or an object
     */
    private boolean set(List path, Object value) {
        return _set(path, value, data)
    }

    public static boolean _set(List path, Object value, Object root) {
        if (!_preparePath(path, root))
            return false

        // Start at root data node
        Object node = root

        for (int i = 0; i < path.size() - 1; ++i) // follow all but last step
        {
            Object step = path.get(i)
            node = node.get(step)
        }

        if ( node instanceof Map )
            node.put(path.get(path.size() - 1), value)
        else if ( node instanceof List ) {
            List nodeAsList = (List) node
            while (nodeAsList.size() < path.get(path.size() - 1))
                nodeAsList.add(null)
            nodeAsList.add(path.get(path.size() - 1), value)
            //node.add(path.get(path.size() - 1), value)
        }
        else
            throw new RuntimeException("Was asked to insert at " + path.get(path.size() - 1) + " in " + node + " and could not match up the container types.")
        return true
    }

    public static boolean _removeLeafObject(List path, Object root) {
        // Start at root data node
        Object node = root

        for (int i = 0; i < path.size() - 1; ++i) // follow all but last step
        {
            Object step = path.get(i)
            node = node.get(step)
        }

        node.remove(path.get(path.size() - 1))
        return true
    }

    private static boolean matchingContainer(Class c1, Class c2) {
        if ((Map.class.isAssignableFrom(c1)) && (Map.class.isAssignableFrom(c2)))
            return true
        if ((List.class.isAssignableFrom(c1)) && (List.class.isAssignableFrom(c2)))
            return true
        return false
    }

    private Object get(List path) {
        return _get(path, data)
    }

    public static Object _get(List path, Object root) {
        // Start at root data node
        Object node = root

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
     * Expand the doc with the supplied extra info.
     *
     */
    void embellish(Map additionalObjects, JsonLd jsonld, boolean filterOutNonChipTerms = true) {
        this.data = jsonld.embellish(this.data, additionalObjects, filterOutNonChipTerms)
    }


    /**
     * Replaces the main ID of this document with 'newId'.
     * Also replaces all derivatives of the old systemId, like for example oldId#it with the corresponding derivative
     * of the new id (newId#it).
     * Finally replaces all document-internal references to
     */
    void deepReplaceId(String newId) {
        String oldId = getCompleteSystemId()

        // If there is no "proper id" use whatever is at the record ID path.
        if (oldId == null)
            oldId = get(recordIdPath)

        deepReplaceIdInternal(oldId, newId, data)
    }

    private void deepReplaceIdInternal(String oldId, String newId, node) {
        if (node instanceof List) {
            for (element in node)
                deepReplaceIdInternal(oldId, newId, element)
        }
        if (node instanceof Map) {
            for (String key : node.keySet()) {
                deepReplaceIdInternal(oldId, newId, node.get(key))
            }
            String nodeId = node["@id"]
            if (nodeId != null && nodeId.startsWith(oldId)) {
                node["@id"] = newId + node["@id"].substring(oldId.length())
            }
        }
    }

    /**
     * Add a new id (URI) to the record and displace the existing URI to
     * the sameAs list. The same thing happens consistently to all
     * derivative (thing/work/etc) IDs.
     */
    public void deepPromoteId(String aliasToPromote) {
        String oldId = get(recordIdPath)
        deepPromoteIdInternal(oldId, aliasToPromote, data)
    }

    private void deepPromoteIdInternal(String oldId, String newId, node) {
        if (node instanceof List) {
            for (element in node)
                deepPromoteIdInternal(oldId, newId, element)
        }
        if (node instanceof Map) {
            for (String key : node.keySet()) {
                deepPromoteIdInternal(oldId, newId, node.get(key))
            }

            String nodeId = node["@id"]
            if (nodeId != null && nodeId.startsWith(oldId)) {
                String expectedNewDerivative = newId + nodeId.substring(oldId.length())

                // If this is not a reference (there's data in the object),
                // move the old @id to a sameAs-list.
                // If however it's only a reference, then replace it outright.
                if (node.keySet().size() > 1) {
                    List sameAsList = (List) node["sameAs"]
                    if (sameAsList == null) {
                        sameAsList = []
                    }

                    boolean alreadyInSameAsList = false
                    for (int i = 0; i < sameAsList.size(); ++i) {
                        Map object = (Map) sameAsList.get(i)
                        String sameAsId = object["@id"]
                        if (sameAsId.equals(nodeId))
                            alreadyInSameAsList = true
                    }
                    if (!alreadyInSameAsList && nodeId != expectedNewDerivative) {
                        sameAsList.add(["@id": nodeId])
                        node.put("sameAs", sameAsList)
                    }
                }

                node["@id"] = expectedNewDerivative
            }
        }
    }

    String getChecksum() {
        long checksum = calculateCheckSum(data, 1)
        return Long.toString(checksum)
    }

    private long calculateCheckSum(node, int depth) {
        long term = 0

        if (node == null)
            return term
        else if (node instanceof String)
            return node.hashCode() * depth
        else if (node instanceof Boolean)
            return node.booleanValue() ? depth : term
        else if (node instanceof Integer)
            return node.intValue() * depth
        else if (node instanceof Map) {
            for (String key : node.keySet()) {
                if ( !key.equals(JsonLd.MODIFIED_KEY) && !key.equals(JsonLd.CREATED_KEY)) {
                    term += key.hashCode() * depth
                    term += calculateCheckSum(node[key], depth + 1)
                }
            }
        }
        else { // node is a list
            int i = 1
            for (entry in node)
                term += calculateCheckSum(entry, depth + (i++))
        }

        return term
    }
}
