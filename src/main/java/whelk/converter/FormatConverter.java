package whelk.converter;

import whelk.Document;
import java.util.List;
import java.util.Map;

public interface FormatConverter {
    public Map convert(Map data, String id);
    public String getRequiredContentType();
    public String getResultContentType();
}
