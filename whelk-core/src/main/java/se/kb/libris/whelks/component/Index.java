package se.kb.libris.whelks.component;

import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.Query;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.result.SearchResult;

public interface Index extends Component {
    /**
     * Indexes an object.
     * @throws IdentifierException if the Document doesn't have an identifier.
     */
    public void index(Document d);
    public void bulkIndex(Iterable<Document> d);
    public SearchResult query(Query query);
}
