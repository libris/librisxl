package se.kb.libris.whelks.component;

import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.Query;
import se.kb.libris.whelks.IndexDocument;
import se.kb.libris.whelks.SearchResult;

public interface Index extends Component {
    public void index(IndexDocument d, String indexName);
    public void bulkIndex(Iterable<IndexDocument> d, String indexName);
    public SearchResult query(Query query, String indexName);
}
