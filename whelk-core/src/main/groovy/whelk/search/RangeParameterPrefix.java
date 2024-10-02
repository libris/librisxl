package whelk.search;

public enum RangeParameterPrefix {
    MIN("min-"),
    MIN_EX("minEx-"),
    MAX("max-"),
    MAX_EX("maxEx-"),
    MATCHES("matches-");

    String prefix;
    String prefix;

    RangeParameterPrefix(String prefix) {
        this.prefix = prefix;
    }

    String prefix() {
        return prefix;
    }

    String asElasticRangeOperator() {
        switch(this) {
            case MIN: return "gte";
            case MIN_EX: return "gt";
            case MAX: return "lte";
            case MAX_EX: return "lt";
            default: throw new IllegalArgumentException("No elastic operator for " + this);
        }
    }
}
