package whelk.search2.querytree;

public record Term(String term) implements Value {
    @Override
    public String queryForm() {
        return term;
    }
}
