package whelk.converter;

import java.io.InputStream;
import java.io.IOException;
import java.util.Map;

import trld.platform.Input;
import trld.trig.Parser;

class TrigToJsonLdParser {
    public static Map parse(InputStream inStream) throws IOException {
        return parse(inStream, (Map) null);
    }

    public static Map parse(InputStream inStream, Map context) throws IOException {
        var data = (Map) Parser.parse(new Input(inStream));
        return (Map) JsonLdShapes.compact(data, context);
    }
}
