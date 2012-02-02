package se.kb.libris.whelks.backends.riak;

import java.net.URI;
import se.kb.libris.whelks.Link;

/**
 * @author marma
 */
public class RiakLink implements Link {
    URI type, from, to;
        
    @Override
    public URI getType() {
        return type;
    }

    @Override
    public URI getFrom() {
        return from;
    }

    @Override
    public URI getTo() {
        return to;
    }
}
