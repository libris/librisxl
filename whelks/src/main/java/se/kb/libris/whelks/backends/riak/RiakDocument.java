package se.kb.libris.whelks.backends.riak;

import java.io.InputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Key;
import se.kb.libris.whelks.Link;

public class RiakDocument implements Document {
    URI identifier = null;
    List<? extends Link> links = new LinkedList<RiakLink>();
    List<? extends Key> keys = new LinkedList<RiakKey>();
    String vclock = null, data = null, contentType = null, format = "UNKNOWN";
    
    public RiakDocument() {
    }
    
    @Override
    public URI getIdentifier() {
        return identifier;
    }
    
    protected void setIdentifier(URI _identifier) {
        identifier = _identifier;
    }

    @Override
    public List<? extends Link> getLinks() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<? extends Key> getKeys() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getData() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public InputStream getDataAsStream() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getContentType() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getFormat() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getSize() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Document setData(String data) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Document setContentType() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Document setFormat(String format) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
