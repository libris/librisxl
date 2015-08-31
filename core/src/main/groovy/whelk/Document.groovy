package whelk

import org.codehaus.jackson.map.*
import org.codehaus.jackson.annotate.JsonIgnore

class Document {
    static final String CREATED_KEY = "created";
    static final String MODIFIED_KEY = "modified";
    static final String DELETED_KEY = "deleted";
    static final String CONTENT_TYPE_KEY = "contentType";

    @JsonIgnore
    static final ObjectMapper mapper = new ObjectMapper()

    String id
    private Map data = [:]
    private Map manifest = [:]
    boolean deleted = false

    Document withContentType(String contentType) {
        manifest.put(CONTENT_TYPE_KEY, contentType)
        return this
    }

    String getDataAsString() {
        return mapper.writeValueAsString(data)
    }

}
