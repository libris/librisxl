package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.Set;

public interface Document {
    public URI getIdentifier();
    public String getVersion();
    public Date getTimestamp();
        
    public byte[] getData();
    public byte[] getData(long offset, long length);
    public InputStream getDataAsStream();
    public InputStream getDataAsStream(long offset, long length);
    public String getContentType();
    public long getSize();
    
    public Set<Link> getLinks();
    public Set<Key> getKeys();
    public Set<Tag> getTags();

    public Document tag(URI type, String value);
    public Document untag(URI type, String value);
    
    public Document withIdentifier(URI uri);
    public Document withData(byte[] data);
    public Document withSize(long size);
    public Document withDataAsStream(InputStream data);
    public Document withContentType(String contentType);
}
