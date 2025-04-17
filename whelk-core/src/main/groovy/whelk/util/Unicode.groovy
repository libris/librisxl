package whelk.util


import java.text.Normalizer
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

class Unicode {
    public static final int MAX_LEVENSHTEIN_LENGTH = 100

    public static final char RIGHT_TO_LEFT_ISOLATE = '\u2067' as char
    public static final char POP_DIRECTIONAL_ISOLATE = '\u2069' as char

    /**
     * Additional characters we want to normalize that are not covered by NFC.
     *
     * Ligatures from the "Alphabetic Presentation Forms" unicode block that are strictly typographical.
     * (but we don't want to touch e.g. æ and ß that are actual letters in some alphabets)
     * https://www.unicode.org/charts/PDF/UFB00.pdf
     * https://en.wikipedia.org/wiki/Orthographic_ligature
     */
    private static final List NORMALIZE_UNICODE_CHARS = [
            'ﬀ', // 'LATIN SMALL LIGATURE FF'
            'ﬃ', // 'LATIN SMALL LIGATURE FFI'
            'ﬄ', // 'LATIN SMALL LIGATURE FFL'
            'ﬁ', // 'LATIN SMALL LIGATURE FI'
            'ﬂ', // 'LATIN SMALL LIGATURE FL'
            'ﬅ', // 'LATIN SMALL LIGATURE LONG S T'
            'ﬆ', // 'LATIN SMALL LIGATURE ST'
    ]

    /** 
     * Characters that should be stripped.
     * 
     * According to the Unicode FAQ, U+FEFF BOM should be treated as ZWNBSP in the middle of data for backwards 
     * compatibility (that use is deprecated in Unicode 3.2). https://www.unicode.org/faq/utf_bom.html#BOM
     * In Libris data analyzed it turned out to always be garbage.
     */
    private static final List STRIP_UNICODE_CHARS = [
            '\ufeff',
    ]
    
    // U+201C LEFT DOUBLE QUOTATION MARK
    // U+201D RIGHT DOUBLE QUOTATION MARK
    private static final Pattern NORMALIZE_DOUBLE_QUOTES = Pattern.compile("[\u201c\u201d]", Pattern.UNICODE_CHARACTER_CLASS)
    
    // U+2060 WORD JOINER
    private static final Pattern LEADING_SPACE = Pattern.compile('^[\\p{Blank}\u2060]+', Pattern.UNICODE_CHARACTER_CLASS)
    private static final Pattern TRAILING_SPACE = Pattern.compile('([\\p{Blank}\u2060]|\\R)+$', Pattern.UNICODE_CHARACTER_CLASS)
    
    private static final Map EXTRA_NORMALIZATION_MAP
    
    static {
        EXTRA_NORMALIZATION_MAP = NORMALIZE_UNICODE_CHARS.collectEntries {
            [(it): Normalizer.normalize(it, Normalizer.Form.NFKC)]
        } + STRIP_UNICODE_CHARS.collectEntries { [(it): ''] }
    }

    private static final Pattern UNICODE_MARK = Pattern.compile('\\p{M}')
    private static final char PRIVATE_USE_AREA = '\uE000' as char

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
            Character.UnicodeScript.THAANA,
    )
    
    static boolean isNormalized(String s) {
        return Normalizer.isNormalized(s, Normalizer.Form.NFC) && !EXTRA_NORMALIZATION_MAP.keySet().any{ s.contains(it) }
    }

    static String normalize(String s) {
        return Normalizer.normalize(s, Normalizer.Form.NFC).replace(EXTRA_NORMALIZATION_MAP)
    }

    static boolean isNormalizedForSearch(String s) {
        return Normalizer.isNormalized(s, Normalizer.Form.NFKC) && isNormalizedDoubleQuotes(s)
    }

    static String normalizeForSearch(String s) {
        return normalizeDoubleQuotes(Normalizer.normalize(s, Normalizer.Form.NFKC))
    }
    
    static boolean isNormalizedDoubleQuotes(String s) {
        !NORMALIZE_DOUBLE_QUOTES.matcher(s).find()
    }
    
    static String normalizeDoubleQuotes(String s) {
        s.replaceAll(NORMALIZE_DOUBLE_QUOTES, '"')
    }

    /**
     * Removes leading and trailing non-"alpha, digit or parentheses".
     */
    static String trimNoise(String s) {
        return trimLeadingNoise(trimLeadingNoise(s).reverse()).reverse()
    }

    /**
     * Removes leading non-"alpha, digit or parentheses".
     */
    static String trimLeadingNoise(String s) {
        def w = /\(\)\p{IsAlphabetic}\p{Digit}/
        def m = s =~ /[^${w}]*(.*)/
        return m.matches() ? m.group(1) : s
    }

    static String trim(String s) {
        s.replaceFirst(LEADING_SPACE, '').replaceFirst(TRAILING_SPACE, '')
    }

    static String stripPrefix(String s, String prefix) {
        s.startsWith(prefix) ? s.substring(prefix.length()) : s
    }

    static String stripSuffix(String s, String suffix) {
        s.endsWith(suffix) ? s.substring(0, s.length() - suffix.length()) : s
    }

    static boolean isRtl(Character.UnicodeScript script) {
        return RTL_SCRIPTS.contains(script);
    }

    static Optional<Character.UnicodeScript> guessScript(String s) {
        s = s.replaceAll(~/\p{IsCommon}|\p{IsInherited}|\p{IsUnknown}/, '')

        if(s.isEmpty()) {
            return Optional.empty()
        }
        
        Map<Character.UnicodeScript, Integer> scores = [:]
        s.codePoints().each {
            var script = Character.UnicodeScript.of(it)
            scores[script] = scores.getOrDefault(script, 0) + 1
        }
        
        var winner = scores.max { it.value }
        var minScore = s.size() / 2
        return winner.value > minScore
            ? Optional.of(winner.key) : Optional.empty()
    }

    static Optional<String> guessIso15924ScriptCode(String s) {
        guessScript(s).flatMap(Unicode::iso15924scriptCode)
    }

    // Character.UnicodeScript has a method for 'ISO 15924 -> UnicodeScript' but not 'UnicodeScript -> ISO 15924'...
    // https://bugs.openjdk.org/browse/JDK-8189951
    static Optional<String> iso15924scriptCode(Character.UnicodeScript script) {
       Optional.ofNullable(JAVA_TO_ISO_15924[script])
    }
    
    static private Map<Character.UnicodeScript, String> JAVA_TO_ISO_15924 = new ConcurrentHashMap<>()
    
    static void add15924scriptCode(String code) {
        try {
            JAVA_TO_ISO_15924[Character.UnicodeScript.forName(code)] = code
        } catch (IllegalArgumentException ignored) {}
    }

    static {
        [
                'Arab',
                'Armn',
                'Bali',
                'Batk',
                'Beng',
                'Cans',
                'Cher',
                'Copt',
                'Cyrl',
                'Cyrs',
                'Deva',
                'Ethi',
                'Geor',
                'Geok',
                'Grek',
                'Gujr',
                'Guru',
                'Hang',
                'Hani',
                'Hans',
                'Hant',
                'Hebr',
                'Hira',
                'Java',
                'Kana',
                'Knda',
                'Khmr',
                'Laoo',
                'Latn',
                'Mlym',
                'Mong',
                'Mymr',
                'Olck',
                'Orya',
                'Sinh',
                'Syrc',
                'Taml',
                'Telu',
                'Thai',
                'Thaa',
                'Tibt',
                'Vaii',
        ].each { add15924scriptCode(it) }
    }

    /**
     * Removes all diacritics from a string, including those of proper letters like å, ä and ö.
     */
    static String removeAllDiacritics(String s) {
        return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll(UNICODE_MARK, '')
    }

    private static def PRESERVE_CHARS = ['å', 'ä', 'ö', 'Å', 'Ä', 'Ö']
    private static Map<String, String> C_SAVE = PRESERVE_CHARS.withIndex().collectEntries() { c, i -> [c, PRIVATE_USE_AREA + i as String]}
    private static Map<String, String> C_RESTORE = C_SAVE.collectEntries { k, v -> [v, k] }

    /**
     * Removes diacritics from a string, but preserves _some_ proper letters like å, ä and ö.
     */
    static String removeDiacritics(String s) {
        removeAllDiacritics(s.replace(C_SAVE)).replace(C_RESTORE)
    }

    /**
     * Computes the Levenshtein distance for two strings
     * Copied from lxl-1931-merge-series-membership.groovy
     */
    static int levenshteinDistance(String a, String b) {
        int rows = a.size() + 1
        int cols = b.size() + 1

        int[][] d = new int[rows][cols]

        for (i in 0..<rows) {
            for (j in 0..<cols) {
                if (i == 0)
                    d[i][j] = j
                else if (j == 0)
                    d[i][j] = i
                else if (a[i-1] == b[j-1]) {
                    d[i][j] = d[i-1][j-1]
                } else {
                    d[i][j] = 1 + [d[i][j-1],  // deletion
                                   d[i-1][j],  // insertion
                                   d[i-1][j-1] // substitution
                                  ].min()
                }
            }
        }

        return d[rows-1][cols-1]
    }

    /**
     * Computes the Damerau–Levenshtein distance for two strings
     * Naive implementation of https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance#Distance_with_adjacent_transpositions
     */
    static int damerauLevenshteinDistance(String a, String b, int maxLen = MAX_LEVENSHTEIN_LENGTH) {
        if (a.size() > maxLen || b.size() > maxLen) {
            throw new IllegalArgumentException("String too long. Max length:${maxLen}. Was:${a.length()}, ${b.length()}")
        }

        int rows = a.size() + 2
        int cols = b.size() + 2

        Map<String, Integer> da = ((a + b) as List).unique().collectEntries(c -> [c, 0])

        int[][] d = new int[rows][cols]

        def maxDist = a.size() + b.size()
        d[0][0] = maxDist
        for (i in 0..a.size()) {
            d[i+1][0] = maxDist
            d[i+1][1] = i
        }
        for (j in 0..b.size()) {
            d[0][j+1] = maxDist
            d[1][j+1] = j
        }

        for (int i = 2 ; i < rows; i++) {
            def ix = i - 2
            def db = 0
            for (int j = 2 ; j < cols ; j++) {
                def jx = j - 2
                def k = da[b[jx]]
                def l = db
                def cost = a[ix] == b[jx] ? 0 : 1
                if (a[ix] == b[jx]) {
                  db = j
                }

                d[i][j] = [d[i]  [j-1] + 1,    // insertion
                           d[i-1][j]   + 1,    // deletion
                           d[i-1][j-1] + cost, // substitution
                           d[k-1][l-1] + (i-k-1) + 1 + (j-l-1) // transposition
                          ].min()
                da[a[ix]] = i
            }
        }

        return d[rows-1][cols-1]
    }

    /**
     * @return ISNI with with four groups of four digits separated by space
     */
    static String formatIsni(String isni)  {
        isni.size() == 16
                ? isni.split("").collate(4).collect{ it.join() }.join(" ")
                : isni
    }
}