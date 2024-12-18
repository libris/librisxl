package whelk.search2.querytree;

import java.util.LinkedHashMap;
import java.util.Map;

import static whelk.JsonLd.TYPE_KEY;

sealed interface InvalidValue extends Value {
    record ForbiddenValue(String string) implements InvalidValue {}
    record AmbiguousValue(String string) implements InvalidValue {}

    @Override
    String string();

    @Override
    default Object description() {
        var m = new LinkedHashMap<String, Object>();
        m.put(TYPE_KEY, "_Invalid");
        m.put("label", string());
        return m;
    }
}
