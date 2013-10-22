package se.kb.libris.whelks

import groovy.transform.TypeChecked
import groovy.util.logging.Slf4j as Log

import java.io.*
import java.net.URI
import java.util.*
import java.nio.ByteBuffer
import java.lang.annotation.*

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
    Map entry // For "technical" metadata about the record, such as contentType, etc.
    Map meta  // For extra metadata about the object, e.g. links and such.

    @JsonIgnore
    ObjectMapper mapper = new ObjectMapper()

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
        return new String(this.data)
    }

    Map getDataAsMap() {
        return mapper.readValue(this.data, Map)
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
        return mapper.writeValueAsString(["identifier":identifier, "meta":meta, "entry":entry])
    }

    /*
     * Convenience methods
     */
    Document withIdentifier(String i) {
        this.identifier = i
        return this
    }
    Document withIdentifier(URI uri) {
        return withIdentifier(uri.toString())
    }

    String getContentType() { entry["contentType"] }

    Document withContentType(String ctype) {
        setContentType(ctype)
        return this
    }

    void setContentType(String ctype) {
        this.entry["contentType"] = ctype
    }

    long getTimestamp() {
        entry.get("timestamp", 0L)
    }

    void setTimestamp(long ts) {
        this.entry["timestamp"] = ts
    }

    List getLinks() {
        return meta.get("links", [])
    }

    Document withData(String dataString) {
        return withData(dataString.getBytes("UTF-8"))
    }

    Document withData(byte[] data) {
        this.data = data
        return this
    }
    Document withEntry(Map entrydata) {
        if (entrydata?.get("identifier", null)) {
            this.identifier = entrydata["identifier"]
            entrydata.remove("identifier")
        }
        if (entrydata) {
            this.entry.putAll(entrydata)
        }
        return this
    }
    Document withMeta(Map metadata) {
        if (metadata) {
            log.info("metadata: $metadata")
            this.meta = metadata
        }
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

    /**
     * Takes either a String or a File as argument.
     */
    Document fromJson(json) {
        try {
            Document newDoc = mapper.readValue(json, Document)
            this.identifier = newDoc.identifier
            this.data = newDoc.data
            this.entry = newDoc.entry
            this.meta = newDoc.meta
        } catch (JsonParseException jpe) {
            throw new DocumentException(jpe)
        }
        return this
    }

}
