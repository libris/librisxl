package whelk.converter;

import whelk.Document;
import java.util.List;

public interface FormatConverter {
    public Document convert(Document doc);
    public String getRequiredContentType();
    public String getResultContentType();
}
