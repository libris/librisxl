package whelk.search2.querytree;

public sealed interface Subpath permits Key, Property {
    // As represented in query string
    String queryForm();
    boolean isType();
    boolean isValid();
    @Override
    String toString();
}