package whelk.component;

import java.util.*;
import whelk.Query;
import whelk.Document;
import whelk.result.SearchResult;
import whelk.exception.WhelkIndexException;

public interface Index extends Component {
    public SearchResult query(Query query);
    public void flush();
    public String getIndexName();
}
