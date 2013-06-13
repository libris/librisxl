package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.Resource;
import se.kb.libris.whelks.RDFDescription;
import se.kb.libris.whelks.Whelk;
import java.util.List;

public interface RDFFormatConverter {
    public List<RDFDescription> convertBulk(List<Resource> doc);
    public List<RDFDescription> convert(Resource doc);
    public String getRequiredContentType();
    public int getOrder();
}
