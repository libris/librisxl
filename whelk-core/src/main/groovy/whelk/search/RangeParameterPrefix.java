package whelk.search;

public enum RangeParameterPrefix {
    MIN("min-"),
    MIN_EX("minEx-"),
    MAX("max-"),
    MAX_EX("maxEx-"),
    MATCHES("matches-");

    final String prefix;

    RangeParameterPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String prefix() {
        return prefix;
    }

    String asElasticRangeOperator() {
        return switch (this) {
            case MIN -> "gte";
            case MIN_EX -> "gt";
            case MAX -> "lte";
            case MAX_EX -> "lt";
            default -> throw new IllegalArgumentException("No elastic operator for " + this);
        };
    }
}
