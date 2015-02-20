package whelk

import groovy.transform.TypeChecked
import groovy.util.logging.Slf4j as Log

import java.io.*
import java.util.*
import java.nio.ByteBuffer
import java.lang.annotation.*
import java.security.MessageDigest

import org.codehaus.jackson.*
import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

import whelk.*
import whelk.component.*
import whelk.exception.*


@Log
class DefaultDocument implements Document {
    protected byte[] data = new byte[0]
    Map entry = [:] // For "technical" metadata about the record, such as contentType, created, etc.
    Map meta  = [:] // For extra metadata about the object, e.g. links and such.

    @JsonIgnore
    private Set<String> identifiers = new LinkedHashSet<String>()
    @JsonIgnore
    private Set<String> datasets = new LinkedHashSet<String>()
    private String checksum = null

    @JsonIgnore
    static final ObjectMapper mapper = new ObjectMapper()

    DefaultDocument() {
        log.trace("### NEW DOCUMENT INSTANTIATED")
        entry = [:]
        meta = [:]
        setCreated(new Date().getTime())
    }

    @JsonIgnore
    String getIdentifier() {
        entry['identifier']
    }

    @JsonIgnore
    List<String> getIdentifiers() {
        def idList = [getIdentifier()]
        idList.addAll(identifiers)
        return idList
    }

    @JsonIgnore
    List<String> getDatasets() {
        def dsList = [getDataset()]
        dsList.addAll(datasets)
        return dsList
    }

    @JsonIgnore
    String getDataAsString() {
        return new String(getData(), "UTF-8")
    }

    @JsonIgnore
    String getChecksum() { checksum }

    String toJson() {
        log.trace("Serializing document.")
        String jsonString = mapper.writeValueAsString(this)
        log.trace("Result of serialization: $jsonString")
        return jsonString
    }

    Map toMap() {
        return mapper.convertValue(this, Map)
    }

    byte[] getData(long offset, long length) {
        byte[] ret = new byte[(int)length]
        System.arraycopy(getData(), (int)offset, ret, 0, (int)length)
        return ret
    }

    byte[] getData() {
        return data
    }

    @JsonIgnore
    String getMetadataAsJson() {
        log.trace("For $identifier. Meta is: $meta, entry is: $entry")
        return mapper.writeValueAsString(["identifier":identifier, "meta":meta, "entry":entry])
    }

    @JsonIgnore
    String getEntryAsJson() {
        return mapper.writeValueAsString(entry)
    }
    @JsonIgnore
    String getMetaAsJson() {
        return mapper.writeValueAsString(meta)
    }

    @JsonIgnore
    String getContentType() { entry[CONTENT_TYPE_KEY] }

    @JsonIgnore
    long getCreated() {
        return (entry.get(CREATED_KEY, 0L))
    }

    @JsonIgnore
    long getModified() {
        return entry.get(MODIFIED_KEY, 0L)
    }

    @JsonIgnore
    Date getModifiedAsDate() {
        return new Date(getModified())
    }

    @JsonIgnore
    int getVersion() {
        return entry.get("version", 0)
    }

    @JsonIgnore
    String getDataset() {
        return entry.get("dataset")
    }

    long updateModified() {
        setModified(new Date().getTime())
        return getModified()
    }

    void setIdentifier(String identifier) {
        entry['identifier'] = identifier
    }

    protected void setCreated(long ts) {
        log.trace("Setting created ts $ts")
        this.entry[CREATED_KEY] = ts
        this.entry["timestamp"] = ts // Hack to prevent _timestamp-errors in older elastic mappings. Safe to remove once all indexes are up to date.
        if (getModified() < 1) {
            this.entry[MODIFIED_KEY] = ts
        }
    }

    void setModified(long mt) {
        log.trace("Updating modified for ${this.identifier} to ${mt}")
        this.entry[MODIFIED_KEY] = mt
    }

    void setVersion(int v) {
        this.entry["version"] = v
    }

    void setData(byte[] data, boolean calcChecksum = true) {
        this.data = data
        if (calcChecksum) {
            calculateChecksum(this.data, meta.toString().getBytes())
        }
    }

    void setDataset(String ds) {
        entry.put("dataset", ds)
    }

    void addIdentifier(String id) {
        identifiers.add(id)
        entry["alternateIdentifiers"] = identifiers
    }

    void addDataset(String ds) {
        datasets = ds
        entry["alternateDatasets"] = datasets
    }

    /*
     * Convenience methods
     */
    Document withIdentifier(String i) {
        this.entry['identifier'] = i
        return this
    }

    Document withContentType(String ctype) {
        setContentType(ctype)
        return this
    }

    void setContentType(String ctype) {
        this.entry[CONTENT_TYPE_KEY] = ctype
    }

    protected Document withCreated(long ts) {
        setCreated(ts)
        return this
    }

    Document withModified(long mt) {
        setModified(mt)
        return this
    }

    Document withVersion(long v) {
        setVersion((int)v)
        return this
    }

    Document withVersion(int v) {
        setVersion(v)
        return this
    }

    Document withData(byte[] data) {
        setData(data)
        return this
    }

    Document withData(String dataString) {
        return withData(dataString.getBytes("UTF-8"))
    }

    Document withDataset(String dataset) {
        setDataset(dataset)
        return this
    }

    /**
     * Convenience method to set data from dictionary, assuming data is to be stored as json.
     */
    void setEntry(Map entryData) {
        log.debug("Clearing entry")
        this.entry = [:]
        withEntry(entryData)
    }

    Document withEntry(Map entrydata) {
        log.debug("withEntry: $entrydata")
        if (entrydata?.containsKey("identifier")) {
            this.identifier = entrydata["identifier"]
        }
        if (entrydata?.containsKey(CREATED_KEY)) {
            setCreated(entrydata.get(CREATED_KEY))
        }
        if (entrydata?.containsKey(MODIFIED_KEY)) {
            setModified(entrydata.get(MODIFIED_KEY))
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
            this.entry.putAll(entrydata)
            if (checksum) {
                this.entry['checksum'] = checksum
            }
        }
        return this
    }
    Document withMeta(Map metadata) {
        if (metadata != null) {
            this.meta = [:]
            this.meta.putAll(metadata)
            calculateChecksum(this.data, this.meta.toString().getBytes())
        }
        return this
    }

    Document setMetaEntry(Map metaEntry) {
        setEntry(metaEntry.entry)
        withMeta(metaEntry.meta)
    }

    Document withMetaEntry(Map metaEntry) {
        withEntry(metaEntry.entry)
        withMeta(metaEntry.meta)
        if (metaEntry.data) {
            withData(metaEntry.data)
        }
        return this
    }

    /**
     * Expects a JSON string containing meta and entry as dictionaries.
     * It's the reverse of getMetadataAsJson().
     */
    Document withMetaEntry(String jsonEntry) {
        Map metaEntry = mapper.readValue(jsonEntry, Map)
        return withMetaEntry(metaEntry)
    }

    Document withMetaEntry(File entryFile) {
        return withMetaEntry(entryFile.getText("utf-8"))
    }

    @JsonIgnore
    boolean isJson() {
        getContentType() ==~ /application\/(\w+\+)*json/ || getContentType() ==~ /application\/x-(\w+)-json/
    }

    @JsonIgnore
    boolean isDeleted() {
        return entry.get("deleted", false)
    }

    protected void calculateChecksum(byte[] databytes, byte[] metabytes) {
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
        this.entry['checksum'] = hashtext
    }
}
