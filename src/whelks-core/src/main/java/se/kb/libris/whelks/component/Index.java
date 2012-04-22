package se.kb.libris.whelks.component;

import java.net.URI;
import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.plugin.Plugin;

public interface Index extends Component {
    public void index(Document d);
    public void delete(URI uri);    
}
