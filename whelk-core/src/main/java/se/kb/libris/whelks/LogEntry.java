package se.kb.libris.whelks;

import java.net.URI;
import java.util.*;

/**
 * @todo Placeholder for something that holds an event for Document history and Whelk feed
 */

public class LogEntry {
    private URI identifier;
    private Date timestamp;

    public LogEntry(URI i, Date t) { 
        this.identifier = i;
        this.timestamp = t;
    }

    public URI getIdentifier() { return identifier; }
    public Date getTimestamp() { return timestamp; }
    public Map<String, String> getInfo() { return new HashMap<String,String>(); }
}
