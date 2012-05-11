package se.kb.libris.whelks;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Date;

public interface Document {
    public URI getIdentifier();
    public String getVersion();
    public Date getTimestamp();
        
    public byte[] getData();
    public byte[] getData(long offset, long length);
    public ByteArrayInputStream getDataAsStream();
    public ByteArrayInputStream getDataAsStream(long offset, long length);
    public String getContentType();
    public long getSize();
    
    public Iterable<? extends Link> getLinks();
    public Iterable<? extends Key> getKeys();
    public Iterable<? extends Tag> getTags();

    public Document tag(URI type, String value);
    public Document untag(URI type, String value);
    public Document withData(byte[] data);
    public Document withDataAsStream(ByteArrayInputStream data);
    public Document withContentType(String contentType);
}
