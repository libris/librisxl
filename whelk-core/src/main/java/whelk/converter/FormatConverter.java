package whelk.converter;

import java.util.Map;

public interface FormatConverter {
    Map convert(Map data, String id);
    String getRequiredContentType();
    String getResultContentType();
}
