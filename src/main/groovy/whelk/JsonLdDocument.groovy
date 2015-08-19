package whelk

import java.time.ZoneId
import java.time.ZonedDateTime

import org.codehaus.jackson.annotate.JsonIgnore

import groovy.util.logging.Slf4j as Log

import whelk.util.JsonLd

@Log
class JsonLdDocument extends JsonDocument {


    final static String ID_KEY = "@id"
    @JsonIgnore
    def timeZone = ZoneId.systemDefault()

    JsonLdDocument() {
        setContentType("application/ld+json")
    }

    protected List<String> findLinks(Map dataMap) {
        List<String> ids = []
        for (entry in dataMap) {
            if (entry.key == ID_KEY && entry.value != identifier) {
                ids.add(entry.value)
            } else if (entry.value instanceof Map) {
                ids.addAll(findLinks(entry.value))
            } else if (entry.value instanceof List) {
                for (l in entry.value) {
                    if (l instanceof Map) {
                        ids.addAll(findLinks(l))
                    }
                }
            }
        }
        return ids
    }

    void setData(byte[] data) {
        super.setData(data)
        def links = findLinks(getDataAsMap())
        //log.info("For ${identifier}, found links: ${links}")
        if (links) {
            this.manifest['links'] = links
        }
    }

    @Override
    protected void setCreated(long ts) {
        log.trace("setCreated in json document ct is: $contentType")
        if (ts != null) {
            super.setCreated(ts)
            def map = getDataAsMap()
            log.trace("Setting created in document data.")
            def time = ZonedDateTime.ofInstant(new Date(ts).toInstant(), timeZone)
            def timestamp = time.format(StandardWhelk.DT_FORMAT)
            if (isFlat()) {
                log.warn("Currently, the flat jsonld has an array in the @graph. When that is fixed (made into a dict), insert the created value into that dict.")
            } else {
                map.put("created", timestamp)
            }
            withData(map)
        }
    }

    @Override
    void setModified(long mt) {
        log.trace("setModified in json document ct is: $contentType")
        if (mt != null) {
            super.setModified(mt)
            def map = getDataAsMap()
            log.trace("Setting modified in document data.")
            def time = ZonedDateTime.ofInstant(new Date(mt).toInstant(), timeZone)
            def timestamp = time.format(StandardWhelk.DT_FORMAT)
            log.warn("Currently, the flat jsonld has an array in the @graph. When that is fixed (made into a dict), insert the modified value into that dict.")
            if (isFlat()) {
            } else {
                map.put("modified", timestamp)
            }
            withData(map)
        }
    }

    boolean isFlat() {
        return JsonLd.isFlat(getDataAsMap())
    }
}
