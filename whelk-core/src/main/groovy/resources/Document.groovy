package se.kb.libris.whelks

import groovy.transform.TypeChecked
import groovy.util.logging.Slf4j as Log

import java.io.*
import java.net.URI
import java.util.*
import java.nio.ByteBuffer
import java.lang.annotation.*
import java.security.MessageDigest

import org.codehaus.jackson.*
import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*


@Log
class Document {
    String identifier
    byte[] data
    Map entry = [:] // For "technical" metadata about the record, such as contentType, timestamp, etc.
    Map meta  = [:] // For extra metadata about the object, e.g. links and such.
    private String checksum = null

    @JsonIgnore
    private static final ObjectMapper mapper = new ObjectMapper()
    @JsonIgnore
    static final TIMESTAMP_KEY = "timestamp"
    @JsonIgnore
    static final MODIFIED_KEY = "modified"

    private long timestamp = 0L
    private long modified

    // store serialized data
    @JsonIgnore
    private Map serializedDataInMap

    /*
     * Constructors
     */
    Document() {
        entry = [:]
        meta = [:]
    }

    Document(String jsonString) {
        entry = [:]
        meta = [:]
        withMetaEntry(jsonString)
    }

    Document(Map jsonMap) {
        entry = [:]
        meta = [:]
        withMetaEntry(jsonMap)
    }

    Document(File jsonFile) {
        entry = [:]
        meta = [:]
        withMetaEntry(jsonFile)
    }

    Document(File datafile, File entryfile) {
        entry = [:]
        meta = [:]
        setData(datafile.readBytes())
        withMetaEntry(entryfile)
    }

    @JsonIgnore
    String getDataAsString() {
        return new String(getData(), "UTF-8")
    }

    @JsonIgnore
    Map getDataAsMap() {
        if (!isJson()) {
            throw new DocumentException("Cannot serialize data as Map. (Content-type is $contentType)")
        }
        if (!serializedDataInMap) {
            log.trace("Serializing data as map")
            this.serializedDataInMap = mapper.readValue(new String(this.data, "UTF-8"), Map)
        }
        return serializedDataInMap
    }

    String getChecksum() { checksum }

    String toJson() {
        return mapper.writeValueAsString(this)
    }

    Map toMap() {
        return mapper.convertValue(this, Map)
    }

    byte[] getData(long offset, long length) {
        byte[] ret = new byte[(int)length]
        System.arraycopy(getData(), (int)offset, ret, 0, (int)length)
        return ret
    }

    @JsonIgnore
    String getMetadataAsJson() {
        log.trace("For $identifier. Meta is: $meta, entry is: $entry")
        return mapper.writeValueAsString(["identifier":identifier, "meta":meta, "entry":entry])
    }

    String getContentType() { entry["contentType"] }

    long getTimestamp() {
        return (entry.get(TIMESTAMP_KEY) ?: 0L)
    }

    long getModified() {
        return (entry.get(MODIFIED_KEY) ?: 0L)
    }

    int getVersion() {
        return (entry.get("version") ?: 0)
    }

    // Setters
    long updateTimestamp() {
        setTimestamp(new Date().getTime())
        log.debug("Updating timestamp for ${this.identifier} to ${timestamp} (${new Date(timestamp)})")
        return this.timestamp
    }

    long updateModified() {
        setModified(new Date().getTime())
        log.trace("Updating modified for ${this.identifier} to ${modified}")
        return this.modified
    }

    void setTimestamp(long ts) {
        this.timestamp = ts
        this.entry[TIMESTAMP_KEY] = ts
    }

    void setModified(long mt) {
        this.modified = mt
        this.entry[MODIFIED_KEY] = mt
    }

    void setVersion(int v) {
        this.entry["version"] = v
    }

    void setData(byte[] data) {
        this.data = data
        // Whenever data is changed, reset serializedDataInMap and checksum
        serializedDataInMap = null
        checksum = null
        calculateChecksum()
        updateModified()
    }

    /*
     * Convenience methods
     */
    Document withIdentifier(String i) {
        this.identifier = i
        this.entry['identifier'] = i
        return this
    }
    Document withIdentifier(URI uri) {
        return withIdentifier(uri.toString())
    }

    Document withContentType(String ctype) {
        setContentType(ctype)
        return this
    }

    void setContentType(String ctype) {
        this.entry["contentType"] = ctype
    }

    Document withTimestamp(long ts) {
        setTimestamp(ts)
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

    /**
     * Convenience method to set data from dictionary, assuming data is to be stored as json.
     */
    Document withData(Map dataMap) {
        return withData(mapper.writeValueAsBytes(dataMap))
    }

    Document setEntry(Map entryData) {
        this.entry = [:]
        return withEntry(entryData)
    }

    Document withEntry(Map entrydata) {
        if (entrydata?.get("identifier", null)) {
            this.identifier = entrydata["identifier"]
        }
        if (entrydata?.get(TIMESTAMP_KEY, null)) {
            setTimestamp(entrydata.get(TIMESTAMP_KEY))
        }
        if (entrydata?.get(MODIFIED_KEY, null)) {
            setModified(entrydata.get(MODIFIED_KEY))
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
        }
        return this
    }

    Document setMetaEntry(Map metaEntry) {
        setEntry(metaEntry.entry)
        withMeta(metaEntry.meta)
        return this
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

    /**
     * Takes either a String or a File as argument.
     */
    static Document fromJson(String json) {
        try {
            Document newDoc = mapper.readValue(json, Document)
            /*
            if (newDoc.data) {
                setData(newDoc.data)
            }
            this.identifier = newDoc.identifier
            this.entry = newDoc.entry
            this.meta = newDoc.meta
            */
            return newDoc
        } catch (JsonParseException jpe) {
            throw new DocumentException(jpe)
        }
        //return this
    }

    private void calculateChecksum() {
        MessageDigest m = MessageDigest.getInstance("MD5")
        m.reset()
        m.update(data)
        byte[] digest = m.digest()
        BigInteger bigInt = new BigInteger(1,digest)
        String hashtext = bigInt.toString(16)
        log.debug("calculated checksum: $hashtext")
        this.checksum = hashtext
        this.entry['checksum'] = hashtext
    }
}
