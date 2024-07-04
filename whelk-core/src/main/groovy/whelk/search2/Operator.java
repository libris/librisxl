package whelk.search2;

// TODO: Add these to vocab (platform terms)
//  e.g. https://id.kb.se/vocab/equals)
public enum Operator {
    EQUALS("equals", "%s:%s"),
    NOT_EQUALS("notEquals", "NOT %s:%s"),
    GREATER_THAN_OR_EQUALS("greaterThanOrEquals", "%s>=%s"),
    GREATER_THAN("greaterThan", "%s>%s"),
    LESS_THAN_OR_EQUALS("lessThanOrEquals", "%s<=%s"),
    LESS_THAN("lessThan", "%s<%s");

    public static final String WILDCARD = "*";

    public final String termKey;
    private final String format;

    Operator(String termKey, String format) {
        this.termKey = termKey;
        this.format = format;
    }

    public String format(String property, String value) {
        return String.format(format, property, value);
    }
}
