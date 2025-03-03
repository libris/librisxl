package whelk.search2.querytree;

import static whelk.JsonLd.TYPE_KEY;

public sealed interface Key extends Subpath permits Key.RecognizedKey, Key.UnrecognizedKey, Key.AmbiguousKey {
    @Override
    default Key key() { return this; }

    @Override
    default boolean isType() {
        return TYPE_KEY.equals(toString());
    }

    @Override
    boolean isValid();

    @Override
    String toString();

    record RecognizedKey(String raw) implements Key {
        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public String toString() {
            return raw;
        }
    }
    record UnrecognizedKey(String raw) implements Key {
        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public String toString() {
            return raw;
        }
    }
    record AmbiguousKey(String raw) implements Key {
        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public String toString() {
            return raw;
        }
    }
}
