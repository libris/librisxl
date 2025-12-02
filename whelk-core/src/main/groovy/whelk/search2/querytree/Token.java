package whelk.search2.querytree;

import whelk.search2.QueryUtil;

public sealed interface Token permits Token.Quoted, Token.Raw {
    String value();
    int offset();
    String formatted();
    boolean isQuoted();

    record Raw(String value, int offset) implements Token {
        public Raw(String value) {
            this(value, -1);
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public String formatted() {
            return value;
        }

        @Override
        public boolean isQuoted() {
            return false;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Raw other && other.value().equals(value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
    
    record Quoted(String value, int offset) implements Token {
        @Override
        public String formatted() {
            return QueryUtil.quote(value);
        }

        @Override
        public boolean isQuoted() {
            return true;
        }

        @Override
        public String toString() {
            return formatted();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Quoted other && other.value().equals(value);
        }

        @Override
        public int hashCode() {
            return formatted().hashCode();
        }
    }
}
