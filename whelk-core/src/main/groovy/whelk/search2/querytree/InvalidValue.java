package whelk.search2.querytree;

import java.util.Map;

import static whelk.JsonLd.TYPE_KEY;

sealed interface InvalidValue extends Value {
    record ForbiddenValue(String string) implements InvalidValue {}
    record AmbiguousValue(String string) implements InvalidValue {}

    @Override
    String string();

    @Override
    default Object description() {
        return Map.of(TYPE_KEY, "_Invalid", "label", string());
    }
}
