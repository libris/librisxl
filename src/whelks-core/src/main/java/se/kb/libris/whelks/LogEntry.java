package se.kb.libris.whelks;

import java.net.URI;
import java.util.Date;
import java.util.Map;

/**
 * @todo Placeholder for something that holds an event for Document history and Whelk feed
 */

public interface LogEntry {
    public URI getIdentifier();
    public Date getTimestamp();
    public Map<String, String> getInfo();
}
