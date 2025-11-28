package whelk.search2.querytree;

import whelk.JsonLd;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static whelk.JsonLd.TYPE_KEY;

public sealed abstract class Key implements Selector, Token permits Key.AmbiguousKey, Key.RecognizedKey, Key.UnrecognizedKey {
    private final String value;
    private final int offset;

    public Key(String value, int offset) {
        this.value = value;
        this.offset = offset;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public int offset() {
        return offset;
    }

    @Override
    public String queryKey() {
        return value;
    }

    @Override
    public String esField() {
        return value;
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
    public List<Selector> getAltPaths(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return List.of(this);
    }

    @Override
    public boolean isType() {
        return value().equals(TYPE_KEY);
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
        return value;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Key other && other.value().equals(value());
    }

    @Override
    public int hashCode() {
        return value().hashCode();
    }

    public static final class RecognizedKey extends Key {
        public RecognizedKey(String value, int offset) {
            super(value, offset);
        }

        public RecognizedKey(String value) {
            this(value, -1);
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public Map<String, Object> definition() {
            // TODO
            return Map.of("ls:indexKey", value());
        }
    }

    public static final class UnrecognizedKey extends Key {
        public UnrecognizedKey(String value, int offset) {
            super(value, offset);
        }

        public UnrecognizedKey(String value) {
            this(value, -1);
        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public Map<String, Object> definition() {
            return Map.of(TYPE_KEY, "_Invalid", "label", value());
        }
    }

    public static final class AmbiguousKey extends Key {
        public AmbiguousKey(String value, int offset) {
            super(value, offset);
        }

        public AmbiguousKey(String value) {
            this(value, -1);
        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public Map<String, Object> definition() {
            return Map.of(TYPE_KEY, "_Invalid", "label", value());
        }
    }
}
