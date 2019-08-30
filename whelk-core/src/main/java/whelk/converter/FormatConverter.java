package whelk.converter;

import java.util.Map;

public interface FormatConverter {
    public Map convert(Map data, String id);
    public String getRequiredContentType();
    public String getResultContentType();
}
