package whelk

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.annotate.JsonIgnore

@Log
class JsonLdDocument extends Document {

    /*
     * Constructors
     */
    JsonLdDocument() {
        super()
    }

    JsonLdDocument(String jsonString) {
        super(jsonString)
    }

    JsonLdDocument(Map jsonMap) {
        super(jsonMap)
    }

    JsonLdDocument(File jsonFile) {
        super(jsonFile)
    }

    JsonLdDocument(File datafile, File entryfile) {
        super(datafile, entryfile)
    }

    @Override
    protected void setModified(long mt) {
        log.trace("Updating modified for ${this.identifier} to ${mt}")
        this.entry[MODIFIED_KEY] = mt
        def map = getDataAsMap()
        map.put("modified", new Date(mt).format("yyyy-MM-dd'T'HH:mm:ss.SZ", TimeZone.default))
        withData(map)
    }
}
