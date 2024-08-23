package whelk.search2.querytree;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public record VocabTerm(String key, Map<String, Object> definition) implements Value {
    public VocabTerm(String string) {
        this(string, Collections.emptyMap());
    }

    @Override
    public String string() {
        return key;
    }

    @Override
    public Object description() {
        return definition;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof VocabTerm && ((VocabTerm) o).key().equals(key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
