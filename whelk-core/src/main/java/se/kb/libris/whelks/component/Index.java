package se.kb.libris.whelks.component;

import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.Query;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.result.SearchResult;
import se.kb.libris.whelks.exception.WhelkIndexException;

public interface Index extends Component {
    /**
     * Indexes a Document.
     * @throws IdentifierException if the Document doesn't have an identifier.
     */
    @Deprecated
    public void index(Document d);
    /**
     * Indexes data into the Index, using the parameters argument for implementation
     * specific parameters
     */
    public void index(List<Map<String,String>> entries) throws WhelkIndexException;
    public void index(byte[] data, Map parameters) throws WhelkIndexException;
    public void flush();

    public SearchResult query(Query query);
}
