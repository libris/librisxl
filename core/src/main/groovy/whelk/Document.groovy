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
    private Map manifest = [:]
    boolean deleted = false
    Date created
    Date modified

    Document(String id, Map data) {
        created = new Date()
        modified = new Date()
        this.id = id
        setData(data)
    }

    Document(Map data, Map manifest) {
        created = new Date()
        modified = new Date()
        withManifest(manifest)
        setData(data)
    }

    Document(String id, Map data, Map manifest) {
        created = new Date()
        modified = new Date()
        withManifest(manifest)
        setData(data)
        this.id = id
    }

    void setContentType(String contentType) {
        withContentType(contentType)
    }

    void setData(Map d) {
        this.data = deepCopy(d)
        calculateChecksum(data, manifest)
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
            created = new Date(entrydata.get(CREATED_KEY))
        }
        if (entrydata?.containsKey(MODIFIED_KEY)) {
            modified = new Date(entrydata.get(MODIFIED_KEY))
        }
        if (entrydata != null) {
            this.manifest.putAll(entrydata)
        }
        calculateChecksum(data, manifest)
        return this
    }

    Document withContentType(String contentType) {
        manifest.put(CONTENT_TYPE_KEY, contentType)
        calculateChecksum(data, manifest)
        return this
    }

    private void calculateChecksum(Map d, Map m) {
        calculateChecksum(mapper.writeValueAsBytes(d), mapper.writeValueAsBytes(m))
    }

    private void calculateChecksum(byte[] databytes, byte[] metabytes) {
        log.trace("Calculating checksum")
        checksum = null
        MessageDigest m = MessageDigest.getInstance("MD5")
        m.reset()
        //byte[] metabytes = meta.toString().getBytes()
        byte[] checksumbytes = new byte[databytes.length + metabytes.length];
        System.arraycopy(databytes, 0, checksumbytes, 0, databytes.length);
        System.arraycopy(metabytes, 0, checksumbytes, databytes.length, metabytes.length);
        m.update(checksumbytes)
        byte[] digest = m.digest()
        BigInteger bigInt = new BigInteger(1,digest)
        String hashtext = bigInt.toString(16)
        log.debug("calculated checksum: $hashtext")
        this.checksum = hashtext
        this.manifest['checksum'] = hashtext
    }
}
