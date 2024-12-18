package whelk.search2.querytree;

import java.util.LinkedHashMap;
import java.util.Map;

import static whelk.JsonLd.TYPE_KEY;

public sealed interface InvalidKey {
    record UnrecognizedKey(String key) implements InvalidKey {}
    record AmbiguousKey(String key) implements InvalidKey {}

    String key();

    default Map<String, Object> getDefinition() {
        var m = new LinkedHashMap<String, Object>();
        m.put(TYPE_KEY, "_Invalid");
        m.put("label", key());
        return m;
    }
}