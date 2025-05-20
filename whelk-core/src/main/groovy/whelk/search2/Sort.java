package whelk.search2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Sort {
    public static Sort DEFAULT_BY_RELEVANCY = new Sort("");

    public enum Order {
        asc,
        desc
    }

    public enum BucketSortKey {
        key("_key"),
        count("_count");

        public final String esKey;

        BucketSortKey(String esKey) {
            this.esKey = esKey;
        }
    }

    private record ParameterOrder(String parameter, Order order) {
        public Map<String, Object> toSortClause(Function<String, String> getSortField) {
            // TODO nested?
            return Map.of(getSortField.apply(parameter), Map.of("order", order));
        }

        public String asString() {
            return order == Order.desc
                    ? "-" + parameter
                    : parameter;
        }
    }

    List<ParameterOrder> parameters;

    private Sort(String sort) {
        parameters = Arrays.stream(sort.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s.startsWith("-")
                        ? new ParameterOrder(s.substring(1), Order.desc)
                        : new ParameterOrder(s, Order.asc))
                .toList();
    }

    static Sort fromString(String s) {
        return (s == null || s.isBlank())
                ? DEFAULT_BY_RELEVANCY
                : new Sort(s);
    }

    public List<Map<String, Object>> getSortClauses(Function<String, String> getSortField) {
        return parameters.stream().map(f -> f.toSortClause(getSortField)).toList();
    }

    String asString() {
        return String.join(",", parameters.stream().map(ParameterOrder::asString).toList());
    }
}
