package whelk.search2.querytree;

import whelk.search.QueryDateTime;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public record YearRange(String min, String max, Token token) implements Value {
    private static String sep() {
        return "-";
    }

    @Override
    public String queryForm() {
        return token != null ? token.toString() : toString();
    }

    @Override
    public String toString() {
        return min + sep() + max;
    }

    public Map<String, Object> toEsInt() {
        return toEs(Integer::parseInt);
    }

    public Map<String, Object> toEsDate() {
        return toEs(s -> QueryDateTime.parse(s).toElasticDateString());
    }


    public static YearRange parse(String s, Token token) {
        if (s.matches("(\\d{4})?-(\\d{4})?") && !s.equals("-")) {
            var hyphenIdx = s.indexOf('-');
            var min = s.substring(0, hyphenIdx);
            var max = s.substring(hyphenIdx + 1);
            return new YearRange(min, max, token);
        }
        return null;
    }

    private Map<String, Object> toEs(Function<String, Object> f) {
        Map<String, Object> m = new HashMap<>();
        if (!min.isEmpty()) {
            m.put("gte", f.apply(min));
        }
        if (!max.isEmpty()) {
            m.put("lte", f.apply(max));
        }
        return m;
    }
}
