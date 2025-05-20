package whelk.search2.querytree;

import java.util.LinkedHashMap;

import static whelk.JsonLd.TYPE_KEY;

sealed public interface InvalidValue extends Value {
    record ForbiddenValue(String raw) implements InvalidValue {
        @Override
        public String toString() {
            return raw;
        }
    }
    record AmbiguousValue(String raw) implements InvalidValue {
        @Override
        public String toString() {
            return raw;
        }
    }

    @Override
    default Object description() {
        var m = new LinkedHashMap<String, Object>();
        m.put(TYPE_KEY, "_Invalid");
        m.put("label", raw());
        return m;
    }

    @Override
    String raw();

    @Override
    default String jsonForm() {
        return raw();
    }
}
