package whelk

import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

import java.security.MessageDigest

class Document {
    static final String CREATED_KEY = "created";
    static final String MODIFIED_KEY = "modified";
    static final String DELETED_KEY = "deleted";
    static final String CONTENT_TYPE_KEY = "contentType";

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
        this.id = id
        this.data = data
    }

    Document(Map data, Map manifest) {
        this.data = data
        withManifest(manifest)
    }

    Document(String id, Map data, Map manifest) {
        this.data = data
        withManifest(manifest)
        this.id = id
    }

    void setContentType(String contentType) {
        withContentType(contentType)
    }

    void setData(Map data) {
        this.data = data
        calculateChecksum(mapper.writeValueAsBytes(data), mapper.writeValueAsBytes([:]))
    }

    String getDataAsString() {
        return mapper.writeValueAsString(data)
    }

    String getIdentifier() { id }

    String getContentType() { manifest[CONTENT_TYPE_KEY] }

    @JsonIgnore
    String getManifestAsJson() {
        return mapper.writeValueAsString(manifest)
    }

    Document withData(Map data) {
        this.data = data
        return this
    }

    void setIdentifiers(List<String> identifiers) {
        manifest.put("alternateIdentifiers", identifiers)
    }

    void addIdentifer(String identifier) {
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
        if (entrydata?.containsKey(CONTENT_TYPE_KEY)) {
            setContentType(entrydata.get(CONTENT_TYPE_KEY))
        }
        if (entrydata?.containsKey("alternateIdentifiers")) {
            this.identifiers = entrydata.get("alternateIdentifiers")
        }
        if (entrydata?.containsKey("alternateDatasets")) {
            this.datasets = entrydata.get("alternateDatasets")
        }
        if (entrydata != null) {
            this.manifest.putAll(entrydata)
            if (checksum) {
                this.manifest['checksum'] = checksum
            }
        }
        return this
    }

    Document withContentType(String contentType) {
        manifest.put(CONTENT_TYPE_KEY, contentType)
        return this
    }

    private void calculateChecksum(byte[] databytes, byte[] metabytes) {
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
