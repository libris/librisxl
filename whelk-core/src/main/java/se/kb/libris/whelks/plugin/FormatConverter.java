package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Whelk;
import java.util.List;

public interface FormatConverter extends Plugin {
    public List<Document> convert(Document doc);
    public String getRequiredContentType();
    public String getRequiredFormat();
}
