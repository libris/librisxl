package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.Set;

public interface Document {
    public URI getIdentifier();
    public void setIdentifier(URI identifier);
    public String getVersion();
    public void setVersion(String version);
    public Date getTimestamp();
    public void setTimestamp(Date date);
        
    public byte[] getData();
    public byte[] getData(long offset, long length);
    public void setData(byte data[]);
    public InputStream getDataAsStream();
    public InputStream getDataAsStream(long offset, long length);
    public void setData(InputStream data);
    public String getContentType();
    public void setContentType(String contentType);
    public long getSize();
    
    public Set<Link> getLinks();
    public Set<Key> getKeys();
    public Set<Description> getDescriptions();    
    public Set<Tag> getTags();
    
    public Document tag(URI type, String value);
    public Document untag(URI type, String value);
}
