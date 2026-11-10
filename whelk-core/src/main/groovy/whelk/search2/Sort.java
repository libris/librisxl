package whelk.search2;

import java.util.Arrays;
import java.util.List;

public record Sort(List<Key> keys) {
    public static final Sort DEFAULT_BY_RELEVANCY = new Sort("");

    public enum Order {
        asc,
        desc
    }

    public record Key(String key, Order order) {
        public String asString() {
            return order == Order.desc
                    ? "-" + key
                    : key;
        }
    }

    private Sort(String sort) {
        this(Arrays.stream(sort.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(s -> s.startsWith("-")
                        ? new Key(s.substring(1), Order.desc)
                        : new Key(s, Order.asc))
                .toList());
    }

    static Sort fromString(String s) {
        return (s == null || s.isBlank())
                ? DEFAULT_BY_RELEVANCY
                : new Sort(s);
    }

    String asString() {
        return String.join(",", keys.stream().map(Key::asString).toList());
    }
}
