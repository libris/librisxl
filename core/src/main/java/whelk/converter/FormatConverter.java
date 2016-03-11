package whelk.converter;

import whelk.Document;
import java.util.List;
import java.util.Map;

public interface FormatConverter {
    public Document convert(Document doc);
    public String getRequiredContentType();
    public String getResultContentType();
}
