package trld.platform;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import com.fasterxml.jackson.jr.ob.JSON;

public class Common {

    public static String resolveIri(String base, String relative) {
        try {
            return new URI(base).resolve(relative).toString();
        } catch (URISyntaxException e) {
            return null;
        }
    }

    public static String uuid4() {
        return java.util.UUID.randomUUID().toString();
    }

    public static void warning(String msg) {
        System.err.println(msg);
    }

    public static Object jsonDecode(String s) {
        try {
            return JSON.std.anyFrom(s);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String jsonEncode(Object o) {
        return jsonEncode(o, false);
    }

    public static String jsonEncode(Object o, boolean pretty) {
        try {
            if (pretty) {
                return JSON.std.with(JSON.Feature.PRETTY_PRINT_OUTPUT).asString(o);
            } else {
                return JSON.std.asString(o);
            }
        } catch (IOException e) {
            return null;
        }
    }

    public static String jsonEncodeCanonical(Object o) {
        return jsonEncode(o); // FIXME: no space separators, do sort keys
    }

}
