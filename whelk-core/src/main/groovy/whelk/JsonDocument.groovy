package whelk

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

import whelk.exception.*

@Log
class JsonDocument extends DefaultDocument {
    // store serialized data
    @JsonIgnore
    protected Map serializedDataInMap

    JsonDocument() {
        super()
    }

    JsonDocument(Document otherDocument) {
        setEntry(otherDocument.getEntry())
        setMeta(otherDocument.getMeta())
        setData(otherDocument.getData())
    }

    void setContentType(String ctype) {
        if (!(ctype ==~ /application\/(\w+\+)*json/ || ctype ==~ /application\/x-(\w+)-json/)) {
            throw new DocumentException("JSON document can't have content-type $ctype")
        }
        super.setContentType(ctype)
    }

    @JsonIgnore
    boolean isJson() { true }

    JsonDocument withData(Map dataMap) {
        return withData(mapper.writeValueAsBytes(dataMap))
    }

    void setData(byte[] data) {
        serializedDataInMap = null
        super.setData(data)
    }

    @JsonIgnore
    Map getDataAsMap() {
        if (!serializedDataInMap) {
            log.trace("Serializing data as map")
            this.serializedDataInMap = mapper.readValue(new String(this.data, "UTF-8"), Map)
        }
        return serializedDataInMap
    }

    @Override
    void setModified(long mt) {
        log.trace("Updating modified for ${this.identifier} to ${mt}")
        this.entry[MODIFIED_KEY] = mt
        if (getContentType() == "application/ld+json") {
            log.trace("Setting modified in document data.")
            def map = getDataAsMap()
            map.put("modified", new Date(mt).format("yyyy-MM-dd'T'HH:mm:ss.SZ", TimeZone.default))
            withData(map)
        }
    }
}
