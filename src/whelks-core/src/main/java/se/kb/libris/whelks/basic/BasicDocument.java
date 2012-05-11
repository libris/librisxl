package se.kb.libris.whelks.basic;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import se.kb.libris.whelks.*;
import se.kb.libris.whelks.exception.WhelkRuntimeException;
import se.kb.libris.whelks.plugin.Pluggable;
import se.kb.libris.whelks.plugin.Plugin;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class BasicDocument implements Document {
    private URI identifier = null;
    private String version = "1", contentType = null;
    private byte[] data = null;
    private long size;
    private List<Link> links = new LinkedList<Link>();
    private List<Key> keys = new LinkedList<Key>();
    private List<Tag> tags = new LinkedList<Tag>();

    private Date timestamp = null;
    
    public URI getIdentifier() {
        return identifier;
    }

    URI setIdentifier(URI _identifier) {
        if (identifier != null)
            throw new WhelkRuntimeException("Identifier cannot be set more than once");
        
        identifier = _identifier;
        
        return identifier;
    }

    public String getVersion() {
        return version;
    }

    public byte[] getData() {
        return data;
    }

    public byte[] getData(long offset, long length) {
        // Tired solution, late friday
        byte[] datapart = new byte[(int)length];
        int j = 0;
        for (int i = (int)offset; i < offset + length; i++) {
            datapart[j++] = data[i];
        }

        return datapart;

    }

    public String getContentType() {
        return contentType;
    }

    public long getSize() {
        return size;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public Iterable<? extends Link> getLinks() {
        return links;
    }

    public Iterable<? extends Key> getKeys() {
        return keys;
    }

    public Iterable<? extends Tag> getTags() {
        return tags;
    }

    public Document tag(URI uri, String value) {
        tags.add(new BasicTag(uri, value));
        
        return this;
    }

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

    public Document withURI(URI uri) {
        this.identifier = uri;
        return this;
    }

    public Document withData(byte[] data) {
        this.data = data;
        this.size = data.length;
        return this;
    }

    public Document withContentType(String contentType) {
        this.contentType = contentType;
        
        return this;
    }

    public InputStream getDataAsStream() {
        return new ByteArrayInputStream(data);
    }

    public InputStream getDataAsStream(long offset, long length) {
        return new ByteArrayInputStream(data, (int)offset, (int)length);
    }

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
            
        }
        
        return this;
    }
}
