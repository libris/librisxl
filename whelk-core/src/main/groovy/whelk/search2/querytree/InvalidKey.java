package whelk.search2.querytree;

import java.util.LinkedHashMap;
import java.util.Map;

import static whelk.JsonLd.TYPE_KEY;

public sealed interface InvalidKey extends PropertyLike {
    record UnrecognizedKey(String name) implements InvalidKey {}
    record AmbiguousKey(String name) implements InvalidKey {}

    default Map<String, Object> definition() {
        var m = new LinkedHashMap<String, Object>();
        m.put(TYPE_KEY, "_Invalid");
        m.put("label", name());
        return m;
    }
}