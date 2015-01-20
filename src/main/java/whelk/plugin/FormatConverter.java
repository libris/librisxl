package whelk.plugin;

import whelk.Document;
import whelk.Whelk;
import java.util.List;

public interface FormatConverter extends Transmogrifier {
    public Document convert(Document doc);
    public String getRequiredContentType();
    public String getResultContentType();
}
