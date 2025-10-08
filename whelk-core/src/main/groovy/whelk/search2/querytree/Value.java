package whelk.search2.querytree;

public sealed interface Value permits DateTime, FreeText, Numeric, Resource {
    // As represented in query string
    String queryForm();

    default boolean isMultiToken() {
        return false;
    }
}