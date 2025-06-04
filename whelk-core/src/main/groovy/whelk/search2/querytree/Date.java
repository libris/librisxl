package whelk.search2.querytree;

public record Date(Token token) implements Value {
    public Date(String date) {
        this(new Token.Quoted(date));
    }

    @Override
    public String queryForm() {
        return token.toString();
    }

    @Override
    public String toString() {
        return token.value();
    }
}
