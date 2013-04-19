package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Document;
import se.kb.libris.whelks.Whelk;
import java.util.List;

public interface IndexFormatConverter extends Plugin {
    public List<Document> convertBulk(List<Document> doc);
    public List<Document> convert(Document doc);
    public String getRequiredContentType();
    public int getOrder();
}
