package se.kb.libris.whelks.index;

import java.util.Map;
import se.kb.libris.whelks.Document;

public interface SearchResult {
    public int numberOfDocuments();
    public Map<String, Facet> getFacets();
    public Document get(int i);
}
