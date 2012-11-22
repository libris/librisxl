package se.kb.libris.whelks.basic;

import java.net.URI;
import se.kb.libris.whelks.Tag;

public class BasicTag implements Tag {
    private URI type;
    private String value;

    public BasicTag(URI _type, String _value) {
        type = _type;
        value = _value;
    }

    public URI getType() {
        return type;
    }

    public String getValue() {
        return value;
    }
}
