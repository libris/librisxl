package whelk.xlql;

// TODO: Add these to vocab (platform terms)
//  e.g. https://id.kb.se/vocab/equals)
public enum Operator {
    EQUALS("equals"),
    NOT_EQUALS("notEquals"),
    GREATER_THAN_OR_EQUALS("greaterThanOrEquals"),
    GREATER_THAN("greaterThan"),
    LESS_THAN_OR_EQUALS("lessThanOrEquals"),
    LESS_THAN("lessThan");

    public final String termKey;

    Operator(String termKey) {
        this.termKey = termKey;
    }
}
