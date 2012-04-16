package se.kb.libris.whelks.basic;

import java.net.URI;
import se.kb.libris.whelks.Link;

public class BasicLink implements Link {
    private URI type, from, to;
    
    public BasicLink(URI _type, URI _from, URI _to) {
        type = _type;
        from = _from;
        to = _to;
    }
    
    public URI getType() {
        return type;
    }

    public URI getFrom() {
        return from;
    }

    public URI getTo() {
        return to;
    }
}
