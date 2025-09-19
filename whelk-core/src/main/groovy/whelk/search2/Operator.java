package whelk.search2;

import java.util.Map;
import java.util.Set;

// TODO: Add these to vocab (platform terms)
//  e.g. https://id.kb.se/vocab/equals)
public enum Operator {
    EQUALS("equals", "%s:%s"),
    NOT_EQUALS("notEquals", "NOT %s:%s"),
    GREATER_THAN_OR_EQUALS("greaterThanOrEquals", "%s>=%s"),
    GREATER_THAN("greaterThan", "%s>%s"),
    LESS_THAN_OR_EQUALS("lessThanOrEquals", "%s<=%s"),
    LESS_THAN("lessThan", "%s<%s");

    private static final Map<Operator, Operator> opposites =  Map.of(
            EQUALS, NOT_EQUALS,
            NOT_EQUALS, EQUALS,
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
                "!=", NOT_EQUALS,
                ">", GREATER_THAN,
                ">=", GREATER_THAN_OR_EQUALS,
                "<", LESS_THAN,
                "<=", LESS_THAN_OR_EQUALS
        );
    }

    public boolean isRange() {
        return rangeOperators().contains(this);
    }

    public static Set<Operator> rangeOperators() {
        return Set.of(Operator.GREATER_THAN_OR_EQUALS, Operator.GREATER_THAN, Operator.LESS_THAN, Operator.LESS_THAN_OR_EQUALS);
    }
}
