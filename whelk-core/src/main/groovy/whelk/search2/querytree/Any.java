package whelk.search2.querytree;

import java.util.Objects;

public sealed abstract class Any implements Node, Value permits Any.EmptyGroup, Any.EmptyString, Any.Wildcard {
    @Override
    public Node getInverse() {
        return new Not(this);
    }

    @Override
    public RdfSubjectType rdfSubjectType() {
        return RdfSubjectType.noType();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Any;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(Any.class);
    }

    public static final class EmptyString extends Any {
        @Override
        public String toQueryString(boolean topLevel) {
            return "";
        }

        @Override
        public String queryForm() {
            return "";
        }
    }

    public static final class EmptyGroup extends Any {
        @Override
        public String toQueryString(boolean topLevel) {
            return "()";
        }

        @Override
        public String queryForm() {
            return "()";
        }
    }

    public static final class Wildcard extends Any {
        @Override
        public String toQueryString(boolean topLevel) {
            return "*";
        }

        @Override
        public String queryForm() {
            return "*";
        }
    }
}
