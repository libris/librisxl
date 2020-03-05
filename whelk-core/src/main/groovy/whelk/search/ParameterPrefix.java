package whelk.search;

public enum ParameterPrefix {
    MIN("min-"),
    MIN_EX("minEx-"),
    MAX("max-"),
    MAX_EX("maxEx-"),
    MATCHES("matches-");

    String prefix;

    ParameterPrefix(String prefix) {
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
