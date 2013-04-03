package se.kb.libris.whelks;

import java.net.URI;

public class Tag {
    private URI type;
    private String value;

    public Tag() {}

    public Tag(URI _type, String _value) {
        type = _type;
        value = _value;
    }

    public URI getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public String toJson() {
        return "{\"type\":\""
              + this.type.toString()
              + "\",\"value\":\""
              + this.value
              + "\"}";
    }
}
