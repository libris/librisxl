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
    Map entry // For "technical" metadata about the record, such as contentType, timestamp, etc.
    Map meta  // For extra metadata about the object, e.g. links and such.
    private String checksum = null

    @JsonIgnore
    ObjectMapper mapper = new ObjectMapper()

    // store serialized data
    @JsonIgnore
    Map serializedDataInMap

    /*
     * Constructors
     */
    Document() {
        entry = ["timestamp":new Date().getTime()]
        meta = [:]
    }

    Document(String jsonString) {
        fromJson(jsonString)
    }

    Document(File jsonFile) {
        fromJson(jsonFile)
    }

    /*
     * Get methods
     */
    String getDataAsString() {
        return new String(this.data, "UTF-8")
    }

    Map getDataAsMap() {
        if (!isJson()) {
            throw new DocumentException("Cannot serialize data as Map. (Not JSON)")
        }
        if (!serializedDataInMap) {
            log.trace("Serializing data as map")
            this.serializedDataInMap = mapper.readValue(new String(this.data, "UTF-8"), Map)
        }
        return serializedDataInMap
    }

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

    String getMetadataAsJson() {
        log.trace("For $identifier. Meta is: $meta, entry is: $entry")
        return mapper.writeValueAsString(["identifier":identifier, "meta":meta, "entry":entry])
    }

    String getContentType() { entry["contentType"] }

    long getTimestamp() {
        entry.get("timestamp", 0L)
    }


    int getVersion() {
        entry.get("version", 0)
    }

    List getLinks() {
        return meta.get("links", [])
    }

    // Setters
    void setTimestamp(long ts) {
        this.entry["timestamp"] = ts
    }

    void setVersion(int v) {
        this.entry["version"] = v
    }

    void setData(byte[] data) {
        this.data = data
        // Whenever data is changed, reset serializedDataInMap
        serializedDataInMap = null
        calculateChecksum()
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

    Document withVersion(int v) {
        setVersion(v)
        return this
    }

    Document withData(String dataString) {
        return withData(dataString.getBytes("UTF-8"))
    }

    Document withData(byte[] data) {
        setData(data)
        return this
    }

    Document withEntry(Map entrydata) {
        if (entrydata?.get("identifier", null)) {
            this.identifier = entrydata["identifier"]
        }
        if (entrydata != null) {
            this.entry = [:]
            this.entry.putAll(entrydata)
            this.entry['checksum'] = checksum
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

    /**
     * Expects a JSON string containing meta and entry as dictionaries.
     * It's the reverse of getMetadataAsJson().
     */
    Document withMetaEntry(String jsonEntry) {
        Map metaEntry = mapper.readValue(jsonEntry, Map)
        withEntry(metaEntry.entry)
        withMeta(metaEntry.meta)
        return this
    }

    Document withLink(String identifier) {
        if (!meta["links"]) {
            meta["links"] = []
        }
        def link = ["identifier":identifier,"type":""]
        meta["links"] << link
        return this
    }

    Document withLink(String identifier, String type) {
        if (!meta["links"]) {
            meta["links"] = []
        }
        def link = ["identifier":identifier,"type":type]
        meta["links"] << link
        return this
    }

    boolean isJson() {
        getContentType() ==~ /application\/(\w+\+)*json/
    }

    /**
     * Takes either a String or a File as argument.
     */
    Document fromJson(json) {
        try {
            Document newDoc = mapper.readValue(json, Document)
            this.identifier = newDoc.identifier
            this.entry = newDoc.entry
            this.meta = newDoc.meta
            setData(newDoc.data)
        } catch (JsonParseException jpe) {
            throw new DocumentException(jpe)
        }
        return this
    }

    /*
    Document mergeEntry(Map entryData) {
        entryData.each { k, v ->
            if (!this.entry.containsKey(k)
                && k != "deleted"
                && k != "version"
                && k != "contentType"
                && k != "checksum"
                && k != "timestamp") {
                log.info("Setting $k = $v")
                this.entry.put(k, v)
            }
        }
        return this
    }
    */

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
