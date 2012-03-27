package se.kb.libris.whelks;

import java.io.InputStream;
import java.net.URI;
import java.util.List;

public interface Document {
    public URI getIdentifier();
    public List<? extends Link> getLinks();
    public List<? extends Key> getKeys();
    public byte[] getData();
    public Document setData(byte[] data);
    public Document setData(InputStream data);
    public InputStream getDataAsStream();
    public String getContentType();
    public Document setContentType();
    public String getFormat();
    public Document setFormat(String format);
    public long getSize();
}
