package whelk

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

import java.security.MessageDigest

@Log
class Document {
    static final String CREATED_KEY = "created";
    static final String MODIFIED_KEY = "modified";
    static final String DELETED_KEY = "deleted";
    static final String DATASET_KEY = "dataset";
    static final String CONTENT_TYPE_KEY = "contentType";
    static final String CHECKUM_KEY = "checksum";

    @JsonIgnore
    static final ObjectMapper mapper = new ObjectMapper()

    private String checksum

    String id
    private Map data = [:]
    private final TreeMap manifest = new TreeMap()
    boolean deleted = false
    Date created
    Date modified

    Document(String id, Map data) {
        this.id = id
        setData(data)
    }

    Document(Map data, Map manifest) {
        withManifest(manifest)
        setData(data)
    }

    Document(String id, Map data, Map manifest) {
        withManifest(manifest)
        setData(data)
        this.id = id
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

    def deepCopy(orig) {
        def bos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(bos)
        oos.writeObject(orig); oos.flush()
        def bin = new ByteArrayInputStream(bos.toByteArray())
        def ois = new ObjectInputStream(bin)
        return ois.readObject()
    }

    @JsonIgnore
    String getDataAsString() {
        return mapper.writeValueAsString(data)
    }

    String getDataset() { manifest[DATASET_KEY] }

    @JsonIgnore
    String getIdentifier() { id }

    @JsonIgnore
    String getContentType() { manifest[CONTENT_TYPE_KEY] }

    @JsonIgnore
    String getManifestAsJson() {
        return mapper.writeValueAsString(manifest)
    }

    String getChecksum() {
        manifest[CHECKUM_KEY]
    }

    Document withData(Map data) {
        setData(data)
        return this
    }

    void addIdentifier(String identifier) {
        manifest.get("alternateIdentifiers", []).add(identifier)
    }

    Document withManifest(Map entrydata) {
        if (entrydata?.containsKey("identifier")) {
            this.id = entrydata["identifier"]
        }
        if (entrydata?.containsKey(CREATED_KEY)) {
            setCreated(entrydata.remove(CREATED_KEY))
        }
        if (entrydata?.containsKey(MODIFIED_KEY)) {
            setModified(entrydata.remove(MODIFIED_KEY))
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
}
