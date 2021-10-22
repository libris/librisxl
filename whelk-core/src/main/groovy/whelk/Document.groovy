package whelk

import groovy.util.logging.Log4j2 as Log
import whelk.util.DocumentUtil
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader
import whelk.util.Unicode

import java.lang.reflect.Type
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.function.Predicate

import static whelk.util.Jackson.mapper

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

    static final List thingPath = ["@graph", 1]
    static final List thingIdPath = ["@graph", 0, "mainEntity", "@id"]
    static final List thingIdPath2 = ["@graph", 1, "@id"]
    static final List thingTypePath = ["@graph", 1, "@type"]
    static final List thingSameAsPath = ["@graph", 1, "sameAs"]
    static final List thingTypedIDsPath = ["@graph", 1, "identifiedBy"]
    static final List thingIndirectTypedIDsPath = ["@graph", 1, "indirectlyIdentifiedBy"]
    static final List thingCarrierTypesPath = ["@graph", 1, "carrierType"]
    static final List thingInSchemePath = ["@graph",1,"inScheme","@id"]
    static final List recordIdPath = ["@graph", 0, "@id"]
    static final List recordTypePath = ["@graph", 0, "@type"]
    static final List workIdPath = ["@graph", 1, "instanceOf", "@id"]
    static final List thingMetaPath = ["@graph", 1, "meta", "@id"]
    static final List recordSameAsPath = ["@graph", 0, "sameAs"]
    static final List recordTypedIDsPath = ["@graph", 0, "identifiedBy"]
    static final List controlNumberPath = ["@graph", 0, "controlNumber"]
    static final List datasetPath = ["@graph", 0, "inDataset"]
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
    static final List categoryPath = ["@graph", 1, "category"]

    URI baseUri = BASE_URI

    public Map data = [:]
    public int version = 0

    Document(Map data) {
        this.data = data
        updateRecordStatus()
    }

    Document(String json) {
        this(mapper.readValue(json, Map))
    }

    Document clone() {
        Map clonedDate = deepCopy(data)
        return new Document(clonedDate)
    }

    void normalizeUnicode() {
        String json = mapper.writeValueAsString(data)
        if (!Unicode.isNormalized(json)) {
            data = mapper.readValue(Unicode.normalize(json), Map)
        }
    }

    boolean trimStrings() {
        DocumentUtil.traverse(data) { value, path ->
            // don't touch indX in _marcUncompleted ' '
            if (value instanceof String && value != ' ' && value != Unicode.trim(value)) {
                return new DocumentUtil.Replace(Unicode.trim(value))
            }
        }
    }
        
    URI getURI() {
        return baseUri.resolve(getShortId())
    }

    String getDataAsString() {
        return mapper.writeValueAsString(data)
    }

    void addInDataset(String dataset) {

        // Make datasetPath point to a list
        preparePath(datasetPath)
        Object datasetList = get(datasetPath)
        if (datasetList == null) {
            datasetList = []
            set(datasetPath, datasetList)
        } else if ( ! (datasetList instanceof List) ) {
            datasetList = [datasetList]
        }

        // Add to list, if not there already
        Map idObject = ["@id" : dataset]
        if (!datasetList.contains(idObject))
            datasetList.add( idObject )
    }

    List getInDataset() {
        def dataset = get(datasetPath)
        if (dataset instanceof List)
            return dataset
        return [dataset]
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

    void setThingInScheme(inScheme) { set(thingInSchemePath, inScheme) }

    String getThingInScheme() { get(thingInSchemePath) }

    void setDescriptionCreator(creator) { set(descriptionCreatorPath, creator) }

    String getDescriptionCreator() { get(descriptionCreatorPath) }

    void setDescriptionLastModifier(modifier) { set(descriptionLastModifierPath, modifier) }

    String getDescriptionLastModifier() { get(descriptionLastModifierPath) }

    void setThingType(thingType) { set(thingTypePath, thingType) }

    String getThingType() { get(thingTypePath) }

    String getRecordType() { get(recordTypePath) }

    String setRecordType(type) { set(recordTypePath, type) }

    String getRecordStatus() { return get(statusPath) }

    void setRecordStatus(status) { set(statusPath, status) }

    void setThingMeta(meta) { set(thingMetaPath, meta) }

    Map getThing() { (Map) get(thingPath) }
    
    void setThing(thing) { _removeLeafObject(thingPath, data); set(thingPath, thing) }

    void setRecordId(id) { set(recordIdPath, id) }

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

    List<String> getIsniValues() {
        return getTypedIDValues(this.&isIsni, thingTypedIDsPath, "value")
    }
    
    static boolean isIsni(Map identifier) { 
         identifier['@type'] == 'ISNI' || identifier.typeNote?.with{ String n -> n.toLowerCase() } == 'isni'
    }

    List<String> getOrcidValues() {
        return getTypedIDValues(this.&isOrcid, thingTypedIDsPath, "value")
    }

    static boolean isOrcid(Map identifier) {
        identifier['@type'] == 'ORCID' || identifier.typeNote?.with{ String n -> n.toLowerCase() } == 'orcid'
    }
    
    private List<String> getTypedIDValues(String typeKey, List<String> idListPath, String valueKey) {
        getTypedIDValues({ it['@type'] == typeKey }, idListPath, valueKey)
    }
        
    private List<String> getTypedIDValues(Predicate<Map> condition, List<String> idListPath, String valueKey) {
        List<String> values = new ArrayList<>()
        List typedIDs = get(idListPath)
        for (Object element : typedIDs) {
            if (!(element instanceof Map))
                continue
            Map map = (Map) element

            if (!condition.test(map)) {
                continue
            }

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

    Instant getModifiedTimestamp() {
        ZonedDateTime.parse(getModified(), DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant()
    }

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
        if (deletedString == null || deletedString != "marc:Deleted")
            return false
        return true
    }

    private void updateRecordStatus() {
        String currentStatus = get(statusPath)
        if (currentStatus != null && currentStatus == "marc:New") {
            String modified = getModified()
            String created = getCreated()
            if (modified != null && created != null && modified != created)
                set(statusPath, "marc:CorrectedOrRevised")
        }
    }

    boolean isHolding(JsonLd jsonld) {
        return ("hold" == LegacyIntegrationTools.determineLegacyCollection(this, jsonld))
    }
    
    boolean isPlaceholder() {
        return getRecordType() == JsonLd.PLACEHOLDER_RECORD_TYPE
    }

    boolean isCacheRecord() {
        return getRecordType() == JsonLd.CACHE_RECORD_TYPE
    }

    String getHeldBySigel() {
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
        addTypedIdentifier(type, identifier, recordTypedIDsPath)
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

    String getWorkType() {
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

    boolean libraryIsRegistrant() {
        Object catObj = get(categoryPath)
        if (catObj == null)
            return false
        List categories
        if (catObj instanceof List)
            categories = catObj
        else
            categories = [catObj]
        return categories.any {
            it["@id"] == "https://id.kb.se/term/bibdb/Registrant"
        }
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
        if (objId == id && map.size() > 1) {
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

    Set<String> getEmbellishments() {
        Set<String> result = new HashSet<>()
        data[JsonLd.GRAPH_KEY].eachWithIndex{ def entry, int i ->
            if (i > 1 && entry[JsonLd.GRAPH_KEY]) {
                result.add(_get(thingIdPath2, entry))
            }
        }
        return result
    }

    /**
     * Return a list of external references in the doc.
     *
     */
    Set<Link> getExternalRefs() {
        return JsonLd.getExternalReferences(this.data)
    }
    
    /**
     * Adds empty structure to the document so that 'path' can be traversed.
     */
    private boolean preparePath(List path) {
        return _preparePath(path, data)
    }

    static boolean _preparePath(List path, Object root) {
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

    static boolean _set(List path, Object value, Object root) {
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

    static boolean _removeLeafObject(List path, Object root) {
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

    static Object _get(List path, Object root, Object defaultTo = null) {
        // Start at root data node
        Object node = root

        for (Object step : path) {
            if ((node instanceof Map) && !(step instanceof String)) {
                log.warn("Needed string as map key, but was given: " + step + ". (path was: " + path + ")")
                return defaultTo
            } else if ((node instanceof List) && !(step instanceof Integer)) {
                log.warn("Needed integer as list index, but was given: " + step + ". (path was: " + path + ")")
                return defaultTo
            }
            node = node[step]

            if (node == null) {
                return defaultTo
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
    void deepPromoteId(String aliasToPromote) {
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
                        if (sameAsId == nodeId)
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

    void applyInverses(JsonLd jsonld) {
        Map thing = get(thingPath)
        jsonld.applyInverses(thing)
        thing.remove(JsonLd.REVERSE_KEY)
    }

    String getChecksum(JsonLd jsonLd) {
        long checksum = calculateCheckSum(data, 1, null, jsonLd)
        return Long.toString(checksum)
    }

    private long calculateCheckSum(node, int depth, parentKey, JsonLd jsonLd) {
        long term = 0

        if (node == null)
            return term
        else if (node instanceof String)
            return node.hashCode() * depth
        else if (node instanceof GString)
            return node.toString().hashCode() * depth
        else if (node instanceof Boolean)
            return node.booleanValue() ? depth : term
        else if (node instanceof Integer)
            return node.intValue() * depth
        else if (node instanceof Long)
            return node.longValue() * depth
        else if (node instanceof Map) {
            for (String key : node.keySet()) {
                if (key != JsonLd.MODIFIED_KEY && key != JsonLd.CREATED_KEY && key != JsonLd.RECORD_STATUS_KEY) {

                    term += key.hashCode() * depth
                    term += calculateCheckSum(node[key], depth + 1, key, jsonLd)
                }
            }
        }
        else if (node instanceof List) {
            int i = 1
            for (entry in node)
                if (isSet(parentKey, jsonLd))
                    term += calculateCheckSum(entry, depth, null, jsonLd)
                else
                    term += calculateCheckSum(entry, depth + (i++), null, jsonLd)
        }
        else {
            return node.hashCode() * depth
        }

        return term
    }

    private static boolean isSet(String key, JsonLd jsonLd) {
        jsonLd && key && jsonLd.isSetContainer(key)
    }

    void replaceLinks(Map<String, String> oldToNew) {
        DocumentUtil.findKey(data, JsonLd.ID_KEY) { value, path ->
            if (oldToNew.containsKey(value)) {
                new DocumentUtil.Replace(oldToNew[(String) value])
            }
        }
    }
}
