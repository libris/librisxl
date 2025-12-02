package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static whelk.JsonLd.TYPE_KEY;

public sealed abstract class Key implements Selector permits Key.AmbiguousKey, Key.RecognizedKey, Key.UnrecognizedKey {
    protected final Token token;

    public Key(Token token) {
        this.token = token;
    }

    @Override
    public String queryKey() {
        return token.formatted();
    }

    @Override
    public String esField() {
        return token.value();
    }

    @Override
    public List<Selector> path() {
        return List.of(this);
    }

    @Override
    public Selector expand(JsonLd jsonLd) {
        return this;
    }

    @Override
    public List<Selector> getAltSelectors(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return List.of(this);
    }

    @Override
    public boolean isType() {
        return token.value().equals(TYPE_KEY);
    }

    @Override
    public boolean valueIsObject() {
        return false;
    }

    @Override
    public boolean mayAppearOnType(String type, JsonLd jsonLd) {
        return false;
    }

    @Override
    public boolean appearsOnType(String type, JsonLd jsonLd) {
        return false;
    }

    @Override
    public boolean indirectlyAppearsOnType(String type, JsonLd jsonLd) {
        return false;
    }

    @Override
    public boolean appearsOnlyOnRecord(JsonLd jsonLd) {
        return false;
    }

    @Override
    public List<String> domain() {
        return List.of();
    }

    @Override
    public List<String> range() {
        return List.of();
    }

    @Override
    public String toString() {
        return token.formatted();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Key other && other.token.equals(this.token);
    }

    @Override
    public int hashCode() {
        return token.hashCode();
    }

    public static final class RecognizedKey extends Key {
        public RecognizedKey(Token token) {
            super(token);
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public Map<String, Object> definition() {
            // TODO
            return Map.of("ls:indexKey", token.value());
        }
    }

    public static final class UnrecognizedKey extends Key {
        public UnrecognizedKey(Token token) {
            super(token);
        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public Map<String, Object> definition() {
            return Map.of(TYPE_KEY, "_Invalid", "label", token.value());
        }
    }

    public static final class AmbiguousKey extends Key {
        public AmbiguousKey(Token token) {
            super(token);
        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public Map<String, Object> definition() {
            return Map.of(TYPE_KEY, "_Invalid", "label", token.value());
        }
    }
}
