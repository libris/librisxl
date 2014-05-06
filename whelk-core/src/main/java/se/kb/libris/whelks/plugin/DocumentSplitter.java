package se.kb.libris.whelks.plugin;

import java.util.List;
import se.kb.libris.whelks.*;

public interface DocumentSplitter extends Plugin {
    public List<Document> split(Document doc);
    public String getRequiredContentType();
    public boolean handles(Document doc);
}
