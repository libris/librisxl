package whelk.search2.querytree.value;

import whelk.search2.Operator;
import whelk.search2.querytree.selector.Property;
import whelk.search2.querytree.node.Condition;

import java.util.Objects;

public sealed abstract class Any implements Value permits Any.EmptyGroup, Any.EmptyString, Any.Wildcard {
    @Override
    public boolean equals(Object obj) {
        return obj instanceof Any;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(Any.class);
    }

    public Condition asNode() {
        return new Condition(new Property.AnyQuery(), Operator.EQUALS, this);
    }

    public static final class EmptyString extends Any {
        @Override
        public String queryForm() {
            return "";
        }
    }

    public static final class EmptyGroup extends Any {
        @Override
        public String queryForm() {
            return "()";
        }
    }

    public static final class Wildcard extends Any {
        @Override
        public String queryForm() {
            return "*";
        }
    }
}
