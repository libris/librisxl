package whelk.search2.querytree;

import java.util.Map;
import java.util.Objects;

import static whelk.JsonLd.TYPE_KEY;
import static whelk.JsonLd.asList;

public final class VocabTerm extends Resource {
    private final String key;
    private final Token token;
    private final Map<String, Object> definition;

    public VocabTerm(String key, Map<String, Object> definition, Token token) {
        this.key = key;
        this.definition = definition;
        this.token = token;
    }

    public VocabTerm(String key, Map<String, Object> definition) {
        this(key, definition, null);
    }

    public String key() {
        return key;
    }

    @Override
    public Map<String, Object> description() {
        return definition;
    }

    @Override
    public String queryForm() {
        return token != null ? token.toString() : key;
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

    @Override
    public String getType() {
        return (String) asList(definition.get(TYPE_KEY)).getFirst();
    }
}
