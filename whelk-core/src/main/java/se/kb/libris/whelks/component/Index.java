package se.kb.libris.whelks.component;

import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.Query;
import se.kb.libris.whelks.IndexDocument;
import se.kb.libris.whelks.SearchResult;

public interface Index extends Component {
    public void index(IndexDocument d);
    public void bulkIndex(Iterable<IndexDocument> d);
    public SearchResult query(Query query);
}
