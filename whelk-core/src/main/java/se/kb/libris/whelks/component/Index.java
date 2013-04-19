package se.kb.libris.whelks.component;

import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.Query;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.SearchResult;

public interface Index extends Component {
    public void index(Document d, String whelkId);
    public void index(Iterable<Document> d, String whelkId);
    public SearchResult query(Query query, String whelkId);

}
