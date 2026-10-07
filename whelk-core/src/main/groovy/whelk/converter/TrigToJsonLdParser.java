package whelk.converter;

import java.io.InputStream;
import java.io.IOException;
import java.util.Map;

import trld.platform.Input;
import trld.trig.Parser;

public class TrigToJsonLdParser {
    public static Map parse(InputStream inStream) throws IOException {
        return parse(inStream, (Map) null);
    }

    public static Map parse(InputStream inStream, Map context) throws IOException {
      return parse(inStream, context, null);
    }

    public static Map parse(InputStream inStream, Map context, String baseIri) throws IOException {
        var data = parseRaw(inStream);
        if (context != null) {
          return (Map) JsonLdShapes.reCompact(data, context, baseIri);
        } else {
          return data;
        }
    }

    public static Map parseRaw(InputStream inStream) throws IOException {
        return (Map) Parser.parse(new Input(inStream));
    }
}
