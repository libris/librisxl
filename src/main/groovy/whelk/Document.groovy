package whelk

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.*

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
    static final ObjectMapper mapper = new ObjectMapper()

    static final List thingIdPath = ["@graph", 0, "mainEntity", "@id"]
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

    void setId(id) { set(recordIdPath, id) }
    String getId() { get(recordIdPath) }

    void setApixExportFailFlag(boolean failed) { set(failedApixExportPath, failed) }
    boolean getApixExportFailFlag() { get(failedApixExportPath) }

    void setControlNumber(controlNumber) { set(controlNumberPath, controlNumber) }
    String getControlNumber() { get(controlNumberPath) }

    void setHoldingFor(holdingFor) { set(holdingForPath, holdingFor) }
    String getHoldingFor() { get(holdingForPath) }

    void setEncodingLevel(encLevel) { set(encLevelPath, encLevel) }
    String getEncodingLevel() { get(encLevelPath) }

    void setCreated(Date created)
    {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(created.toInstant(), ZoneId.systemDefault())
        String formatedCreated = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt)
        set(createdPath, formatedCreated)
    }
    String getCreated() { get(createdPath) }

    void setModified(Date modified)
    {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(modified.toInstant(), ZoneId.systemDefault())
        String formatedModified = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt)
        set(modifiedPath, formatedModified)
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

    void addThingIdentifier(String identifier)
    {
        if (get(thingIdPath) == null)
        {
            set(thingIdPath, identifier)
            return
        }

        preparePath(thingSameAsPath)
        System.out.println(data);
        List sameAsList = get(thingSameAsPath)
        //if (sameAsList != null)

        sameAsList.add(["@id" : identifier])
    }

    /**
     * Adds empty structure to the document so that 'path' can be traversed.
     */
    private void preparePath(List path)
    {
        // Start at root data node
        Object node = data;

        for (int i = 0; i < path.size() - 1; ++i)
        {
            Object step = path.get(i)

            Type nextReplacementType = (path.get(i+1) instanceof Integer) ? ArrayList : HashMap

            Object candidate = null;
            if (node instanceof Map)
                candidate = node.get(step)
            else if (node instanceof List)
            {
                if (node.size() > step)
                    candidate = node.get(step)
            }

            if (candidate == null)
            {
                System.out.println("Adding for step: " + step);
                if (node instanceof Map)
                    node.put(step, nextReplacementType.newInstance())
                else if (node instanceof List)
                {
                    for (int j = 0; j < step+1; ++j)
                        node.add(nextReplacementType.newInstance())
                }
            }
            else if (! candidate instanceof Map)
                log.warn("Structure conflict, path: " + path + " data:\n" + data);

            node = node.get(step)
        }
    }

    private boolean set(List path, value)
    {
        preparePath(path)

        // Start at root data node
        Object node = data;

        for (int i = 0; i < path.size() - 1; ++i) // follow all but last step
        {
            Object step = path.get(i)
            if (node instanceof Map && !step instanceof String)
            {
                log.warn("Needed string as map key, but was given: " + step + ". (path was: " + path + ")")
                return false
            }
            else if (node instanceof List && !step instanceof Integer)
            {
                log.warn("Needed integer as list index, but was given: " + step + ". (path was: " + path + ")")
                return false
            }

            node = node.get(step)
            if (node == null)
            {
                log.warn("Document did not have the required structure for placing a value at: " + path)
                return false
            }
        }

        // The path has now been followed up to the last step, which is now replaced with our new value
        if ( !node instanceof Map )
        {
            log.warn("Document did not have the required structure for placing a value at: " + path)
            return false
        }
        node.put(path.get(path.size()-1), value)
        return true;
    }

    private Object get(List path)
    {
        // Start at root data node
        Object node = data;

        for (Object step : path)
        {
            if (node instanceof Map && !step instanceof String)
            {
                log.warn("Needed string as map key, but was given: " + step + ". (path was: " + path + ")")
                return null;
            }
            else if (node instanceof List && !step instanceof Integer)
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

    static Object deepCopy(Object orig) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream()
        ObjectOutputStream oos = new ObjectOutputStream(bos)
        oos.writeObject(orig); oos.flush()
        ByteArrayInputStream bin = new ByteArrayInputStream(bos.toByteArray())
        ObjectInputStream ois = new ObjectInputStream(bin)
        return ois.readObject()
    }




    // LEGACY:


    void addAliases(Map entry) {
        for (sameAs in asList(entry.get(JSONLD_ALT_ID_KEY))) {
            if (sameAs instanceof Map && sameAs.containsKey(JsonLd.ID_KEY)) {
                String identifier = sameAs.get(JsonLd.ID_KEY)
                int pipeZ = identifier.indexOf(" |z")
                if (pipeZ > 0) {
                    identifier = identifier.substring(0,pipeZ)
                }
                identifier = identifier.trim().replaceAll(/\n|\r/, "")
                identifier = identifier.replaceAll(/\s/, "%20")
                addIdentifier(identifier)
                log.debug("Added ${identifier} to ${getURI()}")
            }
        }
    }

    String getChecksum() {
        return null
    }

    List getQuoted() {
        if (data.containsKey(JsonLd.DESCRIPTIONS_KEY)){
            return getData().get(JsonLd.DESCRIPTIONS_KEY).get("quoted")
        } else {
            def quoted = []
            for (item in data.get(GRAPH_KEY)) {
                if (item.containsKey(GRAPH_KEY)) {
                    quoted << item
                }
            }
            return quoted
        }
        return Collections.emptyList()
    }

    String getQuotedAsString() {
        List quoteds = getQuoted()
        if (quoteds) {
            return mapper.writeValueAsString(quoteds)
        }
        return null
    }

    Document withData(Map data) {
        setData(data)
        return this
    }

    Document addIdentifier(String identifier) {
        Set<String> ids = new HashSet<String>()
        /*ids.addAll(manifest.get(ALTERNATE_ID_KEY) ?: [])
        ids.add(identifier)
        manifest.put(ALTERNATE_ID_KEY, ids)*/
        data[GRAPH_KEY][0][JSONLD_ALT_ID_KEY]
        return this
    }

    Document withIdentifier(String identifier) {
        setId(identifier)
        return this
    }

    Document withContentType(String contentType) {
        manifest.put(CONTENT_TYPE_KEY, contentType)
        return this
    }

    Document inCollection(String ds) {
        if (ds) {
            manifest[COLLECTION_KEY] = ds
        }
        return this
    }

    Document withDeleted(boolean d) {
        setDeleted(d)
        return this
    }

    static boolean isJson(String ct) {
        ct ==~ /application\/(\w+\+)*json/ || ct ==~ /application\/x-(\w+)-json/
    }

    boolean isJson() {
        return isJson(getContentType())
    }

    boolean isFlat() {
        if (isJsonLd()) {
            return JsonLd.isFlat(this.data)
        }
        return false
    }

    boolean isFramed() {
        if (isJsonLd()) {
            return JsonLd.isFramed(this.data)
        }
        return false
    }

    static boolean isJsonLd(String ct) {
        return "application/ld+json" == ct
    }

    boolean isJsonLd() {
        return isJsonLd(getContentType())
    }

    private asList(obj) {
        return obj instanceof List? obj : [obj]
    }


}
