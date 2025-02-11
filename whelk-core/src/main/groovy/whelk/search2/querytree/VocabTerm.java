package whelk.search2.querytree;

import java.util.Map;
import java.util.Objects;

public record VocabTerm(String key, Map<String, Object> definition, String raw) implements Value {
    public VocabTerm(String key, Map<String, Object> definition) {
        this(key, definition, null);
    }

    @Override
    public Object description() {
        return definition;
    }

    @Override
    public String raw() {
        return raw != null ? raw : key;
    }

    @Override
    public String jsonForm() {
        return key;
    }

    @Override
    public String toString() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof VocabTerm vt && vt.key().equals(key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
