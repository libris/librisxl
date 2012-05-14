package se.kb.libris.whelks.basic;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.exception.WhelkRuntimeException;

public class BasicDocument implements Document {
    private URI identifier = null;
    private String version = "1", contentType = null;
    private byte[] data = null;
    private long size;
    private List<Link> links = new LinkedList<Link>();
    private List<Key> keys = new LinkedList<Key>();
    private List<Tag> tags = new LinkedList<Tag>();
    private Date timestamp = null;
    
    public BasicDocument() {
        
    }
    
    @Override
    public URI getIdentifier() {
        return identifier;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public byte[] getData(long offset, long length) {
        byte ret[] = new byte[(int)length];
        System.arraycopy(data, 0, ret, 0, (int)size);

        return ret;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    @Override
    public Iterable<? extends Link> getLinks() {
        return links;
    }

    @Override
    public Iterable<? extends Key> getKeys() {
        return keys;
    }

    @Override
    public Iterable<? extends Tag> getTags() {
        return tags;
    }

    @Override
    public Document tag(URI uri, String value) {
        tags.add(new BasicTag(uri, value));
        
        return this;
    }

    @Override
    public Document untag(URI type, String value) {
        synchronized (tags) {
            ListIterator<Tag> li = tags.listIterator();
            
            while (li.hasNext()) {
                Tag t = li.next();
                
                if (t.getType().equals(t) && t.getValue().equals(value))
                    li.remove();
            }
        }
        
        return this;
    }

    @Override
    public Document withIdentifier(URI uri) {
        this.identifier = uri;
        return this;
    }

    @Override
    public Document withData(byte[] data) {
        this.data = data;
        this.size = data.length;
        return this;
    }

    @Override
    public Document withContentType(String contentType) {
        this.contentType = contentType;
        
        return this;
    }

    @Override
    public Document withSize(long size) {
        this.size = size;
        return this;
    }

    @Override
    public InputStream getDataAsStream() {
        return new ByteArrayInputStream(data);
    }

    @Override
    public InputStream getDataAsStream(long offset, long length) {
        return new ByteArrayInputStream(data, (int)offset, (int)length);
    }

    @Override
    public Document withDataAsStream(InputStream data) {
        byte buf[] = new byte[1024];
        ByteArrayOutputStream bout = new ByteArrayOutputStream(1024);
        
        try {
            int n;
            while ((n = data.read(buf)) != -1)
                bout.write(buf, 0, n);
            
            this.data = bout.toByteArray();
            this.size = this.data.length;
        } catch (java.io.IOException e) {
            throw new WhelkRuntimeException("Error while reading from stream", e);
        }
        
        return this;
    }
}
