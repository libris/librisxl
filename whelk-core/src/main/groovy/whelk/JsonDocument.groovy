package whelk

import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

import whelk.exception.*
import whelk.util.Tools

@Log
class JsonDocument extends DefaultDocument {

    static final DateTimeFormatter DT_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.n]XXX")

    @JsonIgnore
    def timeZone = ZoneId.systemDefault()

    // store serialized data
    @JsonIgnore
    protected Map serializedDataInMap

    @JsonIgnore
    Map metaExtractions = null

    JsonDocument() {
        super()
    }

    JsonDocument(Map metaMapping) {
        this.metaExtractions = metaMapping
    }

    JsonDocument fromDocument(Document otherDocument) {
        setEntry(otherDocument.getEntry())
        setMeta(otherDocument.getMeta())
        setData(otherDocument.getData())
        return this
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
        this.serializedDataInMap = null
        super.setData(data)
        if (getContentType() == "application/ld+json") {
            extractMetaFieldsFromData()
        }
    }

    private void extractMetaFieldsFromData() {
        def d = getDataAsMap()
        if (metaExtractions) {
            metaExtractions.each { dset, rules ->
                if (getDataset() == dset) {
                    rules.each { jsonldpath ->
                        try {
                            def value = Eval.x(d, "x.$jsonldpath")
                            if (value) {
                                meta = Tools.insertAt(meta, jsonldpath, value)
                            }
                        } catch (MissingPropertyException mpe) {
                            log.trace("Unable to set meta property $jsonldpath : $mpe")
                        } catch (NullPointerException npe) {
                            log.warn("Failed to set $jsonldpath for $meta")
                        } catch (Exception e) {
                            log.error("Failed to extract meta info: $e", e)
                            throw e
                        }
                    }
                }
            }
        }
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
            def time = ZonedDateTime.ofInstant(new Date(mt).toInstant(), timeZone)
            def timestamp = time.format(DT_FORMAT)
            map.put("modified", timestamp)
            withData(map)
        }
    }
}
