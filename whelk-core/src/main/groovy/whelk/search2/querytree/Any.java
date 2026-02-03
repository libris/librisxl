package whelk.search2.querytree;

import whelk.JsonLd;
import whelk.search2.ESSettings;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static whelk.search2.QueryUtil.matchAny;

public sealed abstract class Any implements Node, Value permits Any.EmptyGroup, Any.EmptyString, Any.Wildcard {
    @Override
    public Map<String, Object> toEs(ESSettings esSettings) {
        return matchAny();
    }

    @Override
    public ExpandedNode expand(JsonLd jsonLd, Collection<String> rdfSubjectTypes) {
        return ExpandedNode.identity(this);
    }

    @Override
    public Map<String, Object> toSearchMapping(Function<Node, Map<String, String>> makeUpLink) {
        // TODO
        return Map.of();
    }

    @Override
    public Node getInverse() {
        return new Not(this);
    }

    @Override
    public Node reduce(JsonLd jsonLd) {
        return this;
    }

    @Override
    public boolean implies(Node node, JsonLd jsonLd) {
        return implies(node, this::equals);
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
