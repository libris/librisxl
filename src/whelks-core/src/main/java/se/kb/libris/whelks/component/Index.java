package se.kb.libris.whelks.component;

import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.SearchResult;

public interface Index extends Component {
    public void index(Document d);
    public SearchResult query(String query, LinkedHashMap<String, String> sort, Collection<String> highlight);
    public SearchResult fieldQuery(Collection<String> fields, String query, LinkedHashMap<String, String> sort, Collection<String> highlight);
    public long count(String query);
    public void delete(URI uri);    
}
