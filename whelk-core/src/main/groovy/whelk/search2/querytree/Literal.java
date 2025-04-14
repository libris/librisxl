package whelk.search2.querytree;

import whelk.search2.Operator;

public record Literal(String raw) implements Value {
    @Override
    public Object description() {
        return raw;
    }

    @Override
    public String raw() {
        return raw;
    }

    @Override
    public String jsonForm() {
        return raw;
    }

    @Override
    public String toString() {
        return raw;
    }

    public boolean isNumeric() {
        return raw.matches("\\d+");
    }

    public boolean isWildcard() {
        return raw.equals(Operator.WILDCARD);
    }

    public Literal increment() {
        return isNumeric() ? new Literal(Integer.toString(Integer.parseInt(raw) + 1)) : this;
    }

    public Literal decrement() {
        return isNumeric() ? new Literal(Integer.toString(Integer.parseInt(raw) - 1)) : this;
    }
}

