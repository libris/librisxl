package whelk.util

import com.ibm.icu.text.Transliterator

import java.text.Normalizer
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
    
    private static final Map<String, List<Transliterator>> TRANSLITERATORS = [
            'be' : [romanizer('be-Latn-t-be-Cyrl-m0-iso-1995', ['romanization/be-iso.txt', 'romanization/slavic-iso.txt'])],
            'bg' : [romanizer('bg-Latn-t-bg-Cyrl-m0-iso-1995', ['romanization/bg-iso.txt', 'romanization/slavic-iso.txt'])],
            'el' : [romanizer('el-Latn-t-el-Grek-x0-btj', ['romanization/el-btj.txt'])],
            'grc': [romanizer('grc-Latn-t-grc-Grek-x0-skr-1980', ['romanization/grc-skr.txt'])],
            // TODO[ distinguish ISO for slavic languages vs ISO för non-slavic languages? Or describe them with the same entity]?
            'kk' : [romanizer('kk-Latn-t-kk-Cyrl-m0-iso-1995', ['romanization/kk-iso.txt'])],
            'mk' : [romanizer('mk-Latn-t-mk-Cyrl-m0-iso-1995', ['romanization/mk-iso.txt', 'romanization/slavic-iso.txt'])],
            'mn' : [romanizer('mn-Latn-t-mn-Cyrl-x0-lessing', ['romanization/mn-lessing.txt'])],
            'ru' : [romanizer('ru-Latn-t-ru-Cyrl-m0-iso-1995', ['romanization/ru-iso.txt', 'romanization/slavic-iso.txt'])],
            'sr' : [romanizer('sr-Latn-t-sr-Cyrl-m0-iso-1995', ['romanization/sr-iso.txt', 'romanization/slavic-iso.txt'])],
            'uk' : [romanizer('uk-Latn-t-uk-Cyrl-m0-iso-1995', ['romanization/uk-iso.txt', 'romanization/slavic-iso.txt'])],
    ]

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
    
    static Map<String, String> romanize(String s, String langTag) {
        TRANSLITERATORS.getOrDefault(langTag, []).collectEntries {
            [it.getID(), it.transform(s)]
        }
    }
    
    static Set<String> romanizableLangTags() {
        TRANSLITERATORS.keySet()
    }

    private static String readFromResources(String filename) {
        return Unicode.class.getClassLoader()
                .getResourceAsStream(filename).getText("UTF-8")
    }
    
    private static Transliterator romanizer(String id, List<String> filenames) {
        Transliterator.createFromRules(id, filenames.collect(Unicode::readFromResources).join('\n'), Transliterator.FORWARD)
    }
}
