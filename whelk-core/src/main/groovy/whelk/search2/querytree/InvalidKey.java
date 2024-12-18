package whelk.search2.querytree;

import java.util.Map;

import static whelk.JsonLd.TYPE_KEY;

public sealed interface InvalidKey {
    record UnrecognizedKey(String key) implements InvalidKey {}
    record AmbiguousKey(String key) implements InvalidKey {}

    String key();

    default Map<String, Object> getDefinition() {
        return Map.of(TYPE_KEY, "_Invalid", "label", key());
    }
}