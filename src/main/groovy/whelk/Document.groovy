package whelk

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore
import whelk.util.PropertyLoader

@Log
class Document {
    static final String GRAPH_KEY = "@graph"
    static final String ID_KEY = "identifier"
    static final String CREATED_KEY = "created";
    static final String MODIFIED_KEY = "modified";
    static final String DELETED_KEY = "deleted";
    static final String COLLECTION_KEY = "collection";
    static final String CONTENT_TYPE_KEY = "contentType";
    static final String CHECKSUM_KEY = "checksum";
    static final String NON_JSON_CONTENT_KEY = "content"
    static final String ALTERNATE_ID_KEY = "identifiers"
    static final String JSONLD_ALT_ID_KEY = "sameAs"


    static final URI BASE_URI = new URI(PropertyLoader.loadProperties("secret").get("baseUri", "https://libris.kb.se/"))

    @JsonIgnore
    static final ObjectMapper mapper = new ObjectMapper()

    String id
    private Map data = [:]
    protected final TreeMap manifest = new TreeMap()
    boolean deleted = false
    Date created
    Date modified
    int version = 0

    Document() {}

    Document(String id, Map data) {
        setId(id)
        setData(data)
    }

    Document(Map data, Map manifest) {
        withManifest(manifest)
        setData(data)
    }

    Document(String id, Map data, Map manifest) {
        withManifest(manifest)
        setData(data)
        setId(id)
    }

    void setId(id) {
        this.id = id
        this.manifest[ID_KEY] = id
    }

    void setCreated(Date c) {
        if (c) {
            setCreated(c.getTime())
        }
    }

    void setCreated(long c) {
        this.created = new Date(c)
        this.manifest.put(CREATED_KEY, this.created)
    }

    void setModified(Date m) {
        if (m) {
            setModified(m.getTime())
        }
    }

    void setModified(long m) {
        this.modified = new Date(m)
        this.manifest.put(MODIFIED_KEY, this.modified)

    }

    void setContentType(String contentType) {
        withContentType(contentType)
    }

    void setData(Map d) {
        this.data = deepCopy(d)
    }

    void setDeleted(boolean d) {
        deleted = d
        if (deleted) {
            manifest[DELETED_KEY] = deleted
        } else {
            manifest.remove(DELETED_KEY)
        }
    }

    def deepCopy(orig) {
        def bos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(bos)
        oos.writeObject(orig); oos.flush()
        def bin = new ByteArrayInputStream(bos.toByteArray())
        def ois = new ObjectInputStream(bin)
        return ois.readObject()
    }

    URI getURI() {
        return BASE_URI.resolve(id)
    }

    @JsonIgnore
    String getDataAsString() {
        return mapper.writeValueAsString(data)
    }

    Map getData() { data }

    @JsonIgnore
    String getCollection() { manifest[COLLECTION_KEY] }

    @JsonIgnore
    String getIdentifier() { id }

    @JsonIgnore
    String getContentType() { manifest[CONTENT_TYPE_KEY] }

    @JsonIgnore
    String getManifestAsJson() {
        return mapper.writeValueAsString(manifest)
    }

    @JsonIgnore
    List<String> getIdentifiers() {
        if (!manifest.containsKey(ALTERNATE_ID_KEY)) {
            findIdentifiers()
        }
        return manifest.get(ALTERNATE_ID_KEY) as List ?: Collections.emptyList()
    }

    void findIdentifiers() {
        log.debug("Finding identifiers in ${data}")
        addIdentifier(getURI().toString())
        URI docId = JsonLd.findRecordURI(data)
        if (docId) {
            log.debug("Found @id ${docId} in data for ${id}")
            addIdentifier(docId.toString())
        }
        if (data.containsKey(JsonLd.DESCRIPTIONS_KEY)){
            def entry = data.get(JsonLd.DESCRIPTIONS_KEY).get("entry")
            addAliases(entry)
        } else {
            for (entry in data.get(GRAPH_KEY)) {
                log.trace("Walking graph. Current entry: $entry")
                if (entry.containsKey(JsonLd.ID_KEY)) {
                    URI entryURI = null
                    try {
                        entryURI = BASE_URI.resolve(entry[JsonLd.ID_KEY])
                    } catch (IllegalArgumentException iae) {
                        log.warn("Failed to resolve \"${entry[JsonLd.ID_KEY]}\" as URI.")
                    }
                    if (entryURI == getURI()) {
                        addAliases(entry)
                    }
                }
            }
        }
    }

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


    @JsonIgnore
    String getChecksum() {
        manifest[CHECKSUM_KEY]
    }

    Map getManifest() { manifest }

    @JsonIgnore
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

    @JsonIgnore
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
        ids.addAll(manifest.get(ALTERNATE_ID_KEY) ?: [])
        ids.add(identifier)
        manifest.put(ALTERNATE_ID_KEY, ids)
        return this
    }

    Document withIdentifier(String identifier) {
        this.id = identifier
        this.manifest[ID_KEY] = id
        return this
     }

    Document withManifest(Map entrydata) {
        if (entrydata?.containsKey("identifier")) {
            this.id = entrydata["identifier"]
        }
        if (entrydata?.containsKey(ID_KEY)) {
            this.id = entrydata[ID_KEY]
        }
        if (entrydata?.containsKey(CREATED_KEY)) {
            setCreated(entrydata.remove(CREATED_KEY))
        }
        if (entrydata?.containsKey(MODIFIED_KEY)) {
            setModified(entrydata.remove(MODIFIED_KEY))
        }
        if (entrydata?.containsKey(DELETED_KEY)) {
            deleted = entrydata[DELETED_KEY]
        }
        if (entrydata != null) {
            this.manifest.putAll(entrydata)
        }
        return this
    }

    Document withContentType(String contentType) {
        manifest.put(CONTENT_TYPE_KEY, contentType)
        return this
    }

    Document inCollection(String ds) {
        manifest[COLLECTION_KEY] = ds
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
