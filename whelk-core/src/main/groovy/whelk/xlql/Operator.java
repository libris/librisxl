package whelk.xlql;

// TODO: Add these to vocab (platform terms)
//  e.g. https://id.kb.se/vocab/equals)
public enum Operator {
    EQUAL("Equal"),
    NOT_EQUAL("NotEqual"),
    GREATER_THAN_OR_EQUAL("GreaterThanOrEqual"),
    GREATER_THAN("GreaterThan"),
    LESS_THAN_OR_EQUAL("LessThanOrEqual"),
    LESS_THAN("LessThan");

    public String termKey;

    Operator(String termKey) {
        this.termKey = termKey;
    }
}
