package whelk.search2;

import java.util.Map;

// TODO: Add these to vocab (platform terms)
//  e.g. https://id.kb.se/vocab/equals)
public enum Operator {
    EQUALS("equals", "%s:%s"),
    GREATER_THAN_OR_EQUALS("greaterThanOrEquals", "%s>=%s"),
    GREATER_THAN("greaterThan", "%s>%s"),
    LESS_THAN_OR_EQUALS("lessThanOrEquals", "%s<=%s"),
    LESS_THAN("lessThan", "%s<%s");

    private static final Map<Operator, Operator> opposites =  Map.of(
            GREATER_THAN_OR_EQUALS, LESS_THAN,
            GREATER_THAN, LESS_THAN_OR_EQUALS,
            LESS_THAN_OR_EQUALS, GREATER_THAN,
            LESS_THAN, GREATER_THAN_OR_EQUALS
    );

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

    public Operator getInverse() {
        return opposites.get(this);
    }

    public static Map<String, Operator> symbolMappings() {
        return Map.of(
                "=", EQUALS,
                ">", GREATER_THAN,
                ">=", GREATER_THAN_OR_EQUALS,
                "<", LESS_THAN,
                "<=", LESS_THAN_OR_EQUALS
        );
    }

    public boolean isRange() {
        return this != EQUALS;
    }
}
