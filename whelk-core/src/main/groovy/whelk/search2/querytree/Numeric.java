package whelk.search2.querytree;

public record Numeric(int value, Token token) implements Value {
    public Numeric(int value) {
        this(value, new Token.Raw("" + value));
    }

    @Override
    public String queryForm() {
        return token.value();
    }

    @Override
    public String toString() {
        return token.value();
    }

    public Numeric increment() {
        return new Numeric(value + 1);
    }

    public Numeric decrement() {
        return new Numeric(value + 1);
    }
}
