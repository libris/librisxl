package whelk.search2.querytree;

import whelk.search2.QueryUtil;

import java.util.Optional;

public sealed interface Token permits Key.AmbiguousKey, Key.RecognizedKey, Key.UnrecognizedKey, Token.Quoted, Token.Raw {
    String value();
    int offset();

    default boolean isQuoted() {
        return false;
    }

    record Raw(String value, int offset) implements whelk.search2.querytree.Token {
        public Raw(String value) {
            this(value, -1);
        }

        @Override
        public String toString() {
            return value;
        }
    }
    
    record Quoted(String value, int offset) implements whelk.search2.querytree.Token {
        @Override
        public String toString() {
            return QueryUtil.quote(value);
        }

        @Override
        public boolean isQuoted() {
            return true;
        }
    }
}
