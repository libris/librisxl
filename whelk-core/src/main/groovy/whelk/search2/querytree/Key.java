package whelk.search2.querytree;

import static whelk.JsonLd.TYPE_KEY;

public sealed interface Key extends Subpath permits Key.AmbiguousKey, Key.RecognizedKey, Key.UnrecognizedKey {
    @Override
    default String queryKey() {
        return value();
    }

    @Override
    default String indexKey() {
        return value();
    }

    @Override
    default boolean isType() {
        return TYPE_KEY.equals(value());
    }

    String value();

    record RecognizedKey(String value, int offset) implements Key, Token {
        public RecognizedKey(String value) {
            this(value, -1);
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    record UnrecognizedKey(String value, int offset) implements Key, Token {
        public UnrecognizedKey(String value) {
            this(value, -1);
        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    record AmbiguousKey(String value, int offset) implements Key, Token {
        public AmbiguousKey(String value) {
            this(value, -1);
        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public String toString() {
            return value;
        }
    }
}
