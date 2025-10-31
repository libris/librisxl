package whelk.search2.querytree;

public sealed interface Value permits DateTime, FreeText, Numeric, Resource, Term, YearRange {
    // As represented in query string
    String queryForm();

    default boolean isMultiToken() {
        return false;
    }

    default boolean isRangeOpCompatible() {
        return false;
    }
}