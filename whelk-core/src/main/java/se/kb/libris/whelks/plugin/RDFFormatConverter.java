package se.kb.libris.whelks.plugin;

import se.kb.libris.whelks.RDFDescription;
import se.kb.libris.whelks.Whelk;
import java.util.List;

public interface RDFFormatConverter extends Plugin {
    public List<RDFDescription> convertBulk(List<RDFDescription> doc);
    public List<RDFDescription> convert(RDFDescription doc);
    public String getRequiredContentType();
    public int getOrder();
}
