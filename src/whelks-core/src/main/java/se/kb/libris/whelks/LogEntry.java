package se.kb.libris.whelks;

import java.net.URI;
import java.util.Date;
import java.util.Map;

public interface LogEntry {
    public URI getIdentifier();
    public Date getTimestamp();
    public Map<String, String> getInfo();
}
