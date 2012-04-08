package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.List;

public interface Document {
    public URI getIdentifier();
    public List<? extends Link> getLinks();
    public List<? extends Key> getKeys();
    public byte[] getData();
    public String getContentType();
    public String getFormat();
    public long getSize();
}
