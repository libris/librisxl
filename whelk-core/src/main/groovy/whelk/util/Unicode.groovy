package whelk.util

import java.text.Normalizer
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern

class Unicode {

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
    private static final Pattern TRAILING_SPACE = Pattern.compile('[\\p{Blank}\u2060]+$', Pattern.UNICODE_CHARACTER_CLASS)
    
    private static final Map EXTRA_NORMALIZATION_MAP
    
    static {
        EXTRA_NORMALIZATION_MAP = NORMALIZE_UNICODE_CHARS.collectEntries {
            [(it): Normalizer.normalize(it, Normalizer.Form.NFKC)]
        } + STRIP_UNICODE_CHARS.collectEntries { [(it): ''] }
    }
    
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
                'Java',
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
}
