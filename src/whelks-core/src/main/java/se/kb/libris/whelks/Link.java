package se.kb.libris.whelks;

import java.util.Map;
import java.net.URI;

import se.kb.libris.whelks.exception.*;

public class Link {
    private String type = "default";
    private URI identifier;

    public Link() {}

    public Link(URI id) {
        setIdentifier(id);
    }
    public Link(URI id, String t) {
        setIdentifier(id);
        setType(t);
    }
    /*
    public Link(Map map) {
        if (map != null) {
            if (map.containsKey("type")) {
                this.type = (String)map.get("type");
            }
            if (map.containsKey("identifier")) {
                Object o = map.get("identifier");
                if (o instanceof URI) {
                    this.identifier = (URI)identifier;
                } else if (o instanceof String) {
                    try {
                        this.identifier = new URI((String)o);
                    } catch (java.net.URISyntaxException us) {
                        throw new WhelkRuntimeException(us);
                    }
                }
            }
        }
    }
    */

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
}
