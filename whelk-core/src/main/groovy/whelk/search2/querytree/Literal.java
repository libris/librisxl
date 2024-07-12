package whelk.search2.querytree;

public record Literal(String string) implements Value {
    @Override
    public Object description() {
        return string;
    }

    public boolean isNumeric() {
        return string.matches("\\d+");
    }

    public Literal increment() {
        return isNumeric() ? new Literal(Integer.toString(Integer.parseInt(string()) + 1)) : this;
    }

    public Literal decrement() {
        return isNumeric() ? new Literal(Integer.toString(Integer.parseInt(string()) - 1)) : this;
    }
}

