package se.kb.libris.whelks.basic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
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

    public String getContentType() {
        return contentType;
    }

    public long getSize() {
        return size;
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

    public Document withData(byte[] data) {
        this.data = data;
        
        return this;
    }

    public Document withContentType(String contentType) {
        this.contentType = contentType;
        
        return this;
    }

    public ByteArrayInputStream getDataAsStream() {
        return new ByteArrayInputStream(data);
    }

    public Document withDataAsStream(ByteArrayInputStream data) {
        byte buf[] = new byte[1024];
        ByteArrayOutputStream bout = new ByteArrayOutputStream(1024);
        
        try {
            int n;
            while ((n = data.read(buf)) != -1)
                bout.write(buf, 0, n);
            
            this.data = bout.toByteArray();
        } catch (java.io.IOException e) {
            
        }
        
        return this;
    }
}
