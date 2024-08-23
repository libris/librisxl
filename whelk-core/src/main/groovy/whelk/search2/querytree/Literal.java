package whelk.search2.querytree;

import whelk.search2.Operator;

public record Literal(String string) implements Value {
    @Override
    public Object description() {
        return string;
    }

    public boolean isNumeric() {
        return string.matches("\\d+");
    }

    public boolean isWildcard() {
        return string.equals(Operator.WILDCARD);
    }

    public Literal increment() {
        return isNumeric() ? new Literal(Integer.toString(Integer.parseInt(string()) + 1)) : this;
    }

    public Literal decrement() {
        return isNumeric() ? new Literal(Integer.toString(Integer.parseInt(string()) - 1)) : this;
    }
}

