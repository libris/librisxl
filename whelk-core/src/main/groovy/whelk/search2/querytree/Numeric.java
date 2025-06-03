package whelk.search2.querytree;

public record Numeric(int value) implements Value {
    @Override
    public String queryForm() {
        return "" + value;
    }

    @Override
    public String toString() {
        return "" + value;
    }

    public Numeric increment() {
        return new Numeric(value + 1);
    }

    public Numeric decrement() {
        return new Numeric(value + 1);
    }
}
