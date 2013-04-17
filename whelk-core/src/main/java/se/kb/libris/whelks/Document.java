package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.Set;
import java.util.Map;

public interface Document {
    public URI getIdentifier();
    public void setIdentifier(URI identifier);
    public String getVersion();
    public void setVersion(String version);
    public long getTimestamp();
    public void setTimestamp(long timestamp);
    public byte[] getData();
    public String getContentType();
    public void setContentType(String contentType);
    public String getFormat();
    public void setFormat(String format);
    public long getSize();
    public Set<Link> getLinks();
    public Set<Description> getDescriptions();
    public Set<Tag> getTags();

    public InputStream getDataAsStream();
    public InputStream getDataAsStream(long offset, long length);
    public byte[] getData(long offset, long length);
    public String getDataAsString();
    public Map getDataAsJson();
    public Date getTimestampAsDate();
    public Document updateTimestamp();

    //public void setData(byte data[]);
    //public void setData(InputStream data);

    public Document withIdentifier(String uri);
    public Document withIdentifier(URI uri);
    public Document withData(String dataString);
    public Document withData(byte[] data);
    public Document withSize(long size);
    public Document withContentType(String contentType);
    public Document withFormat(String format);

    public String toJson();

    public Document tag(URI type, String value);
    public void untag(URI type, String value);

    public Document withLink(Link link);
    public Document withLink(String identifier);
    public Document withLink(URI identifier);
    public Document withLink(String identifier, String type);
    public Document withLink(URI identifier, String type);
}
