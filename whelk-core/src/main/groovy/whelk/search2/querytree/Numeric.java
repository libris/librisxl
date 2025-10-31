package whelk.search2.querytree;

public record Numeric(long value, Token token) implements Value {
    public Numeric(long value) {
        this(value, null);
    }

    @Override
    public String queryForm() {
        return "" + value;
    }

    @Override
    public boolean isRangeOpCompatible() {
        return true;
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

    @Override
    public boolean equals(Object o) {
        return o instanceof Numeric n && n.value() == value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    public static Numeric parse(String s, Token token) {
        if (s.matches("\\d+")) {
            try {
                return new Numeric(Long.parseLong(s), token);
            } catch (NumberFormatException ignored) {
            }
        }
        return null;
    }
}
