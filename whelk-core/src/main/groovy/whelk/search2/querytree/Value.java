package whelk.search2.querytree;

public sealed interface Value permits DateTime, FreeText, Numeric, Resource, Term, Value.Any, YearRange {
    // As represented in query string
    String queryForm();

    default boolean isMultiToken() {
        return false;
    }

    default boolean isRangeOpCompatible() {
        return false;
    }

    record Any(Token token) implements Value {
        @Override
        public String queryForm() {
            return token.value().isEmpty() ? "()" : token().formatted();
        }
    }
}