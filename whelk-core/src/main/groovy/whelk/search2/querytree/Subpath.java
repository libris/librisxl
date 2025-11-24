package whelk.search2.querytree;

public sealed interface Subpath permits Key, Property {
    // As represented in query string
    String queryKey();
    String indexKey();
    boolean isType();
    boolean isValid();
}