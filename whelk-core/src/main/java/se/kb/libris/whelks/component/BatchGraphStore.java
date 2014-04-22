package se.kb.libris.whelks.component;

import java.util.Map;
import java.net.URI;
import se.kb.libris.whelks.RDFDescription;

public interface BatchGraphStore extends Component {
    public int getOptimumBatchSize();
    public void update(URI uri, RDFDescription d);
    public void batchUpdate(Map<URI, RDFDescription> batch);
}
