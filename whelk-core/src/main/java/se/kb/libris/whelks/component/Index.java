package se.kb.libris.whelks.component;

import java.net.URI;
import java.util.*;
import se.kb.libris.whelks.Query;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.result.SearchResult;
import se.kb.libris.whelks.exception.WhelkIndexException;

public interface Index extends Component {
    public SearchResult query(Query query);
    public void flush();
}
