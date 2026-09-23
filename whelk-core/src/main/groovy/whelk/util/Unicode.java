package whelk.util;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class Unicode {
    public static final int MAX_LEVENSHTEIN_LENGTH = 100;

    public static final char RIGHT_TO_LEFT_ISOLATE = '\u2067';
    public static final char POP_DIRECTIONAL_ISOLATE = '\u2069';

    /**
     * Additional characters we want to normalize that are not covered by NFC.
     *
     * Ligatures from the "Alphabetic Presentation Forms" unicode block that are strictly typographical.
     * (but we don't want to touch e.g. æ and ß that are actual letters in some alphabets)
     * https://www.unicode.org/charts/PDF/UFB00.pdf
     * https://en.wikipedia.org/wiki/Orthographic_ligature
     */
    private static final List<String> NORMALIZE_UNICODE_CHARS = List.of(
            "ﬀ", // 'LATIN SMALL LIGATURE FF'
            "ﬃ", // 'LATIN SMALL LIGATURE FFI'
            "ﬄ", // 'LATIN SMALL LIGATURE FFL'
            "ﬁ", // 'LATIN SMALL LIGATURE FI'
            "ﬂ", // 'LATIN SMALL LIGATURE FL'
            "ﬅ", // 'LATIN SMALL LIGATURE LONG S T'
            "ﬆ"  // 'LATIN SMALL LIGATURE ST'
    );

    /**
     * Characters that should be stripped.
     *
     * According to the Unicode FAQ, U+FEFF BOM should be treated as ZWNBSP in the middle of data for backwards
     * compatibility (that use is deprecated in Unicode 3.2). https://www.unicode.org/faq/utf_bom.html#BOM
     * In Libris data analyzed it turned out to always be garbage.
     */
    private static final List<String> STRIP_UNICODE_CHARS = List.of(
            "\ufeff"
    );

    // U+201C LEFT DOUBLE QUOTATION MARK
    // U+201D RIGHT DOUBLE QUOTATION MARK
    private static final Pattern NORMALIZE_DOUBLE_QUOTES = Pattern.compile("[\u201c\u201d]", Pattern.UNICODE_CHARACTER_CLASS);

    // U+2060 WORD JOINER
    private static final Pattern LEADING_SPACE = Pattern.compile("^[\\p{Blank}\u2060]+", Pattern.UNICODE_CHARACTER_CLASS);
    private static final Pattern TRAILING_SPACE = Pattern.compile("([\\p{Blank}\u2060]|\\R)+$", Pattern.UNICODE_CHARACTER_CLASS);

    private static final Map<String, String> EXTRA_NORMALIZATION_MAP;

    static {
        EXTRA_NORMALIZATION_MAP = new LinkedHashMap<>();
        for (String c : NORMALIZE_UNICODE_CHARS) {
            EXTRA_NORMALIZATION_MAP.put(c, Normalizer.normalize(c, Normalizer.Form.NFKC));
        }
        for (String c : STRIP_UNICODE_CHARS) {
            EXTRA_NORMALIZATION_MAP.put(c, "");
        }
    }

    private static final Pattern UNICODE_MARK = Pattern.compile("\\p{M}");
    private static final char PRIVATE_USE_AREA = '\uE000';

    private static final Pattern COMMON_INHERITED_UNKNOWN = Pattern.compile("\\p{IsCommon}|\\p{IsInherited}|\\p{IsUnknown}");

    private static final Pattern NOISE = Pattern.compile("[^\\(\\)\\p{IsAlphabetic}\\p{Digit}]*(.*)");

    private static final EnumSet<Character.UnicodeScript> RTL_SCRIPTS = EnumSet.of(
            Character.UnicodeScript.ADLAM,
            Character.UnicodeScript.ARABIC,
            Character.UnicodeScript.AVESTAN,
            Character.UnicodeScript.HEBREW,
            Character.UnicodeScript.MANDAIC,
            Character.UnicodeScript.MENDE_KIKAKUI,
            Character.UnicodeScript.NKO,
            Character.UnicodeScript.OLD_NORTH_ARABIAN,
            Character.UnicodeScript.OLD_SOUTH_ARABIAN,
            Character.UnicodeScript.SAMARITAN,
            Character.UnicodeScript.SYRIAC,
            Character.UnicodeScript.THAANA
    );

    public static boolean isNormalized(String s) {
        return Normalizer.isNormalized(s, Normalizer.Form.NFC)
                && EXTRA_NORMALIZATION_MAP.keySet().stream().noneMatch(s::contains);
    }

    public static String normalize(String s) {
        String result = Normalizer.normalize(s, Normalizer.Form.NFC);
        for (Map.Entry<String, String> e : EXTRA_NORMALIZATION_MAP.entrySet()) {
            result = result.replace(e.getKey(), e.getValue());
        }
        return result;
    }

    public static boolean isNormalizedForSearch(String s) {
        return Normalizer.isNormalized(s, Normalizer.Form.NFKC) && isNormalizedDoubleQuotes(s);
    }

    public static String normalizeForSearch(String s) {
        return normalizeDoubleQuotes(Normalizer.normalize(s, Normalizer.Form.NFKC));
    }

    public static boolean isNormalizedDoubleQuotes(String s) {
        return !NORMALIZE_DOUBLE_QUOTES.matcher(s).find();
    }

    public static String normalizeDoubleQuotes(String s) {
        return NORMALIZE_DOUBLE_QUOTES.matcher(s).replaceAll("\"");
    }

    /**
     * Removes leading and trailing non-"alpha, digit or parentheses".
     */
    public static String trimNoise(String s) {
        return reverse(trimLeadingNoise(reverse(trimLeadingNoise(s))));
    }

    /**
     * Removes leading non-"alpha, digit or parentheses".
     */
    public static String trimLeadingNoise(String s) {
        var m = NOISE.matcher(s);
        return m.matches() ? m.group(1) : s;
    }

    public static String trim(String s) {
        return TRAILING_SPACE.matcher(LEADING_SPACE.matcher(s).replaceFirst("")).replaceFirst("");
    }

    public static String stripPrefix(String s, String prefix) {
        return s.startsWith(prefix) ? s.substring(prefix.length()) : s;
    }

    public static String stripSuffix(String s, String suffix) {
        return s.endsWith(suffix) ? s.substring(0, s.length() - suffix.length()) : s;
    }

    public static boolean isRtl(Character.UnicodeScript script) {
        return RTL_SCRIPTS.contains(script);
    }

    public static Optional<Character.UnicodeScript> guessScript(String s) {
        s = COMMON_INHERITED_UNKNOWN.matcher(s).replaceAll("");

        if (s.isEmpty()) {
            return Optional.empty();
        }

        Map<Character.UnicodeScript, Integer> scores = new HashMap<>();
        s.codePoints().forEach(cp -> {
            var script = Character.UnicodeScript.of(cp);
            scores.merge(script, 1, Integer::sum);
        });

        var winner = scores.entrySet().stream().max(Map.Entry.comparingByValue()).orElseThrow();
        double minScore = s.length() / 2.0;
        return winner.getValue() > minScore
                ? Optional.of(winner.getKey()) : Optional.empty();
    }

    public static Optional<String> guessIso15924ScriptCode(String s) {
        return guessScript(s).flatMap(Unicode::iso15924scriptCode);
    }

    // Character.UnicodeScript has a method for 'ISO 15924 -> UnicodeScript' but not 'UnicodeScript -> ISO 15924'...
    // https://bugs.openjdk.org/browse/JDK-8189951
    public static Optional<String> iso15924scriptCode(Character.UnicodeScript script) {
        return Optional.ofNullable(JAVA_TO_ISO_15924.get(script));
    }

    private static final Map<Character.UnicodeScript, String> JAVA_TO_ISO_15924 = new ConcurrentHashMap<>();

    public static void add15924scriptCode(String code) {
        try {
            JAVA_TO_ISO_15924.put(Character.UnicodeScript.forName(code), code);
        } catch (IllegalArgumentException ignored) {}
    }

    static {
        List<String> codes = List.of(
                "Arab",
                "Armn",
                "Bali",
                "Batk",
                "Beng",
                "Cans",
                "Cher",
                "Copt",
                "Cyrl",
                "Cyrs",
                "Deva",
                "Ethi",
                "Geor",
                "Geok",
                "Grek",
                "Gujr",
                "Guru",
                "Hang",
                "Hani",
                "Hans",
                "Hant",
                "Hebr",
                "Hira",
                "Java",
                "Kana",
                "Knda",
                "Khmr",
                "Laoo",
                "Latn",
                "Mlym",
                "Mong",
                "Mymr",
                "Olck",
                "Orya",
                "Sinh",
                "Syrc",
                "Taml",
                "Telu",
                "Thai",
                "Thaa",
                "Tibt",
                "Vaii"
        );
        codes.forEach(Unicode::add15924scriptCode);
    }

    /**
     * Removes all diacritics from a string, including those of proper letters like å, ä and ö.
     */
    public static String removeAllDiacritics(String s) {
        return UNICODE_MARK.matcher(Normalizer.normalize(s, Normalizer.Form.NFD)).replaceAll("");
    }

    private static final List<String> PRESERVE_CHARS = List.of("å", "ä", "ö", "Å", "Ä", "Ö");
    private static final Map<String, String> C_SAVE = buildSaveMap();
    private static final Map<String, String> C_RESTORE = invert(C_SAVE);

    private static Map<String, String> buildSaveMap() {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < PRESERVE_CHARS.size(); i++) {
            map.put(PRESERVE_CHARS.get(i), String.valueOf((char) (PRIVATE_USE_AREA + i)));
        }
        return map;
    }

    private static Map<String, String> invert(Map<String, String> map) {
        Map<String, String> inverted = new LinkedHashMap<>();
        map.forEach((k, v) -> inverted.put(v, k));
        return inverted;
    }

    /**
     * Removes diacritics from a string, but preserves _some_ proper letters like å, ä and ö.
     */
    public static String removeDiacritics(String s) {
        return replace(removeAllDiacritics(replace(s, C_SAVE)), C_RESTORE);
    }

    private static String replace(String s, Map<String, String> replacements) {
        for (Map.Entry<String, String> e : replacements.entrySet()) {
            s = s.replace(e.getKey(), e.getValue());
        }
        return s;
    }

    /**
     * Computes the Levenshtein distance for two strings
     * Copied from lxl-1931-merge-series-membership.groovy
     */
    public static int levenshteinDistance(String a, String b) {
        int rows = a.length() + 1;
        int cols = b.length() + 1;

        int[][] d = new int[rows][cols];

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                if (i == 0)
                    d[i][j] = j;
                else if (j == 0)
                    d[i][j] = i;
                else if (a.charAt(i - 1) == b.charAt(j - 1)) {
                    d[i][j] = d[i - 1][j - 1];
                } else {
                    d[i][j] = 1 + Math.min(d[i][j - 1],          // deletion
                              Math.min(d[i - 1][j],              // insertion
                                       d[i - 1][j - 1]));        // substitution
                }
            }
        }

        return d[rows - 1][cols - 1];
    }

    public static int damerauLevenshteinDistance(String a, String b) {
        return damerauLevenshteinDistance(a, b, MAX_LEVENSHTEIN_LENGTH);
    }

    /**
     * Computes the Damerau–Levenshtein distance for two strings
     * Naive implementation of https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance#Distance_with_adjacent_transpositions
     */
    public static int damerauLevenshteinDistance(String a, String b, int maxLen) {
        if (a.length() > maxLen || b.length() > maxLen) {
            throw new IllegalArgumentException("String too long. Max length:" + maxLen + ". Was:" + a.length() + ", " + b.length());
        }

        int rows = a.length() + 2;
        int cols = b.length() + 2;

        // Positions are offset by one against the logical algorithm ("d[-1][-1]" is d[0][0] etc.),
        // so "no previous occurrence" is 1, not 0.
        Map<Character, Integer> da = new HashMap<>();
        for (char c : (a + b).toCharArray()) {
            da.putIfAbsent(c, 1);
        }

        int[][] d = new int[rows][cols];

        int maxDist = a.length() + b.length();
        d[0][0] = maxDist;
        for (int i = 0; i <= a.length(); i++) {
            d[i + 1][0] = maxDist;
            d[i + 1][1] = i;
        }
        for (int j = 0; j <= b.length(); j++) {
            d[0][j + 1] = maxDist;
            d[1][j + 1] = j;
        }

        for (int i = 2; i < rows; i++) {
            int ix = i - 2;
            int db = 1;
            for (int j = 2; j < cols; j++) {
                int jx = j - 2;
                int k = da.get(b.charAt(jx));
                int l = db;
                int cost = a.charAt(ix) == b.charAt(jx) ? 0 : 1;
                if (a.charAt(ix) == b.charAt(jx)) {
                    db = j;
                }

                d[i][j] = Math.min(Math.min(d[i][j - 1] + 1,                           // insertion
                                            d[i - 1][j] + 1),                          // deletion
                          Math.min(d[i - 1][j - 1] + cost,                             // substitution
                                   d[k - 1][l - 1] + (i - k - 1) + 1 + (j - l - 1)));  // transposition
                da.put(a.charAt(ix), i);
            }
        }

        return d[rows - 1][cols - 1];
    }

    /**
     * @return ISNI with with four groups of four digits separated by space
     */
    public static String formatIsni(String isni) {
        if (isni.length() != 16) {
            return isni;
        }
        List<String> groups = new ArrayList<>();
        for (int i = 0; i < 16; i += 4) {
            groups.add(isni.substring(i, i + 4));
        }
        return String.join(" ", groups);
    }

    private static final Predicate<String> ISBN10 = Pattern.compile("(?:\\d-?){9}(?:\\d|X)").asMatchPredicate();
    private static final Predicate<String> ISBN13 = Pattern.compile("(?:978|979)(?:-?\\d){10}").asMatchPredicate();

    public static boolean looksLikeIsbn(String s) {
        return ISBN10.test(s) || ISBN13.test(s);
    }

    private static String reverse(String s) {
        return new StringBuilder(s).reverse().toString();
    }
}
