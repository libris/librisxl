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

    @JsonIgnore
    Map metaExtractions = null

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

    protected JsonDocument withMetaExtrationMapping(Map mapping) {
        this.metaExtractions = mapping
        return this
    }

    JsonDocument withData(Map dataMap) {
        return withData(mapper.writeValueAsBytes(dataMap))
    }

    void setData(byte[] data) {
        this.serializedDataInMap = null
        super.setData(data)
        if (getContentType() == "application/ld+json") {
            extractMetaFieldsFromData()
        }
    }

    private void extractMetaFieldsFromData() {
        def d = getDataAsMap()
        if (metaExtractions) {
            metaExtractions.each { dset, rule ->
                if (getDataset() == dset) {
                    rule.each { metakey, jsonldpath ->
                        try {
                            def p = d + "." + jsonldpath
                            meta.put(metakey, Eval.me(p))
                            log.info("Meta is now: $meta")
                        } catch (NullPointerException npe) {
                            log.warn("Failed to set $jsonldpath for $metakey")
                        }
                    }
                }
            }
        }
        /*
        if (getDataset() == "hold") {
            if (d?.about?.heldBy?.notation) {
                meta.sigel = d.about.heldBy.notation
            }
        }
        */
    }

    @JsonIgnore
    Map getDataAsMap() {
        if (!serializedDataInMap) {
            log.trace("Serializing data as map")
            this.serializedDataInMap = mapper.readValue(new String(getData(), "UTF-8"), Map)
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
