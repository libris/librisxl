package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Resource;
import se.kb.libris.whelks.IndexDocument;
import se.kb.libris.whelks.Whelk;
import java.util.List;

public interface IndexFormatConverter extends Plugin {
    public List<IndexDocument> convertBulk(List<Resource> doc);
    public List<IndexDocument> convert(Resource doc);
    public String getRequiredContentType();
    public int getOrder();
}
