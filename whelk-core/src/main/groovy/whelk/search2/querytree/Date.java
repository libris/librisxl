package whelk.search2.querytree;

public record Date(String date) implements Value {
    @Override
    public String queryForm() {
        return date;
    }

    @Override
    public String toString() {
        return date;
    }
}
