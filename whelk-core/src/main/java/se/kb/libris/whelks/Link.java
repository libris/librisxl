package se.kb.libris.whelks;

import java.util.Map;
import java.net.URI;

import se.kb.libris.whelks.exception.*;

public class Link {
    private String type = "";
    private URI identifier;

    public Link() {}

    public Link(URI id) {
        setIdentifier(id);
    }
    public Link(URI id, String t) {
        setIdentifier(id);
        setType(t);
    }

    public String getType() {
        return this.type;
    }
    public URI getIdentifier() {
        return this.identifier;
    }

    public void setType(String t) {
        this.type = t;
    }
    public void setIdentifier(URI id) {
        this.identifier = id;
    }

    public String toJson() {
        return "{\"type\":\""
              + this.type
              + "\",\"identifier\":\""
              + this.identifier.toString()
              + "\"}";
    }
}
