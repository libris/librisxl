package se.kb.libris.whelks;

import java.io.ByteArrayInputStream;
import java.net.URI;

/**
 * @todo MM 20120414 Support streams instead of/in addition to byte arrays
 */
public interface Document {
    public URI getIdentifier();
    
    public byte[] getData();
    //public ByteArrayInputStream getDataAsStream();
    public String getContentType();
    public long getSize();
    
    public Iterable<? extends Link> getLinks();
    public Iterable<? extends Key> getKeys();
    public Iterable<? extends Tag> getTags();

    public Document tag(URI uri, String value);
    public Document untag(URI uri, String value);
    public Document withData(byte[] data);
    //public Document withData(ByteArrayInputStream data);
    public Document withContentType(String contentType);
}
