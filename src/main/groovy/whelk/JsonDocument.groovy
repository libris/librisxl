package whelk

import groovy.util.logging.Slf4j as Log

import java.time.ZoneId
import java.time.ZonedDateTime

import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

import whelk.exception.*
import whelk.util.Tools

@Log
class JsonDocument extends DefaultDocument {

    // store serialized data
    @JsonIgnore
    protected Map serializedDataInMap
    @JsonIgnore
    def timeZone = ZoneId.systemDefault()

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
        setData(mapper.writeValueAsBytes(dataMap))
        this.serializedDataInMap = dataMap
        return this
    }

    void setData(byte[] data) {
        this.serializedDataInMap = null
        super.setData(data)
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
    long updateModified() {
        long mt = super.updateModified()
        if (getContentType() == "application/ld+json") {
            def map = getDataAsMap()
            if (map.containsKey("modified")) {
                log.trace("Setting modified in document data.")
                def time = ZonedDateTime.ofInstant(new Date(mt).toInstant(), timeZone)
                def timestamp = time.format(StandardWhelk.DT_FORMAT)
                map.put("modified", timestamp)
                withData(map)
            }
        }
        return mt
    }

    @Override
    void addIdentifier(String id) {
        super.addIdentifier(id)
        if (getContentType() == "application/ld+json") {
            def docDataMap = getDataAsMap()
            if (docDataMap.containsKey("sameAs")) {
                def sameAsList = new HashSet<String>()
                sameAsList.addAll(docDataMap['sameAs'])
                sameAsList.add(id)
                docDataMap.put("sameAs", sameAsList)
            } else {
                docDataMap.put("sameAs", [id])
            }
            withData(docDataMap)
        }
    }

    @Override
    protected void calculateChecksum(byte[] databytes, byte[] metabytes) {
        if (getData().length > 0) {
            log.trace("Normalizing json data before checksum calculation")
            def dataMap = getDataAsMap()
            setData(mapper.writeValueAsBytes(dataMap), false)
            // Removed modified from data before checksum calculation
            dataMap.remove("modified")
            databytes = mapper.writeValueAsBytes(dataMap)
        }
        super.calculateChecksum(databytes, metabytes)
    }
}
