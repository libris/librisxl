package whelk.search2.querytree;

import java.util.LinkedHashMap;

import static whelk.JsonLd.TYPE_KEY;

sealed public interface InvalidValue extends Value {
    record ForbiddenValue(Token token) implements InvalidValue {
        @Override
        public String toString() {
            return token.value();
        }
    }
    record AmbiguousValue(Token token) implements InvalidValue {
        @Override
        public String toString() {
            return token.value();
        }
    }

    Token token();

    @Override
    default Object description() {
        var m = new LinkedHashMap<String, Object>();
        m.put(TYPE_KEY, "_Invalid");
        m.put("label", raw());
        return m;
    }

    @Override
    default String raw() {
        return token().value();
    }

    @Override
    default String jsonForm() {
        return raw();
    }
}
