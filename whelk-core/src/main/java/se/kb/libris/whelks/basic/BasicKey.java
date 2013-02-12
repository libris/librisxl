package se.kb.libris.whelks.basic;

import java.net.URI;
import se.kb.libris.whelks.Key;

public class BasicKey implements Key {
    private URI type;
    private String value;

    public BasicKey(URI _type, String _value) {
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
