package whelk.util

import java.text.Normalizer

class Unicode {

    /**
     * Additional characters we want to normalize that are not covered by NFC.
     *
     * Ligatures from the "Alphabetic Presentation Forms" unicode block that are strictly typographical.
     * (but we don't want to touch e.g. æ and ß that are actual letters in some alphabets)
     * https://www.unicode.org/charts/PDF/UFB00.pdf
     * https://en.wikipedia.org/wiki/Orthographic_ligature
     */
    private static final List FORBIDDEN_UNICODE_CHARS = [
            'ﬀ', // 'LATIN SMALL LIGATURE FF'
            'ﬃ', // 'LATIN SMALL LIGATURE FFI'
            'ﬄ', // 'LATIN SMALL LIGATURE FFL'
            'ﬁ', // 'LATIN SMALL LIGATURE FI'
            'ﬂ', // 'LATIN SMALL LIGATURE FL'
            'ﬅ', // 'LATIN SMALL LIGATURE LONG S T'
            'ﬆ', // 'LATIN SMALL LIGATURE ST'
    ]

    private static final Map EXTRA_NORMALIZATION_MAP

    static {
        EXTRA_NORMALIZATION_MAP = FORBIDDEN_UNICODE_CHARS.collectEntries {
            [(it): Normalizer.normalize(it, Normalizer.Form.NFKC)]
        }
    }

    static boolean isNormalized(String s) {
        return Normalizer.isNormalized(s, Normalizer.Form.NFC) && !EXTRA_NORMALIZATION_MAP.keySet().any{ s.contains(it) }
    }

    static String normalize(String s) {
        return Normalizer.normalize(s, Normalizer.Form.NFC).replace(EXTRA_NORMALIZATION_MAP)
    }

    static boolean isNormalizedForSearch(String s) {
        return Normalizer.isNormalized(s, Normalizer.Form.NFKC)
    }

    static String normalizeForSearch(String s) {
        return Normalizer.normalize(s, Normalizer.Form.NFKC)
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
}
