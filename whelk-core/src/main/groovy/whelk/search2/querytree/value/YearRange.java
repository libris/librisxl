package whelk.search2.querytree.value;

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

    public static YearRange parse(String s, Token token) {
        if (s.matches("(\\d{4})?-(\\d{4})?") && !s.equals("-")) {
            var hyphenIdx = s.indexOf('-');
            var min = s.substring(0, hyphenIdx);
            var max = s.substring(hyphenIdx + 1);
            return new YearRange(min, max, token);
        }
        return null;
    }
}
