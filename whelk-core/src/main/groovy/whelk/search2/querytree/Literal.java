package whelk.search2.querytree;

import whelk.search2.Operator;

public record Literal(Token token) implements Value {
    public Literal(String value) {
        this(new Token.Raw(value));
    }

    @Override
    public Object description() {
        return token.value();
    }

    @Override
    public String raw() {
        return token.value();
    }

    @Override
    public String jsonForm() {
        return token.value();
    }

    @Override
    public String toString() {
        return token.value();
    }

    public boolean isNumeric() {
        return token.value().matches("\\d+");
    }

    public boolean isWildcard() {
        return token.value().equals(Operator.WILDCARD);
    }

    public Literal increment() {
        return isNumeric() ? new Literal(new Token.Raw(Integer.toString(Integer.parseInt(token.value()) + 1), token.offset())) : this;
    }

    public Literal decrement() {
        return isNumeric() ? new Literal(new Token.Raw(Integer.toString(Integer.parseInt(token.value()) - 1), token.offset())) : this;
    }
}

