package whelk.util

import java.text.Normalizer

class Unicode {
    private static final List FORBIDDEN_UNICODE_CHARS = [
            'ﬀ', // 'LATIN SMALL LIGATURE FF'
            'ﬃ', // 'LATIN SMALL LIGATURE FFI'
            'ﬄ', // 'LATIN SMALL LIGATURE FFL'
            'ﬁ', // 'LATIN SMALL LIGATURE FI'
            'ﬂ', // 'LATIN SMALL LIGATURE FL'
            'ﬅ', // 'LATIN SMALL LIGATURE LONG S T'
            'ﬆ', // 'LATIN SMALL LIGATURE ST'
    ]

    private static final Map FORBIDDEN_UNICODE_MAP

    static {
        FORBIDDEN_UNICODE_MAP = FORBIDDEN_UNICODE_CHARS.collectEntries {
            [(it): Normalizer.normalize(it, Normalizer.Form.NFKC)]
        }
    }

    static boolean isNormalized(String s) {
        return Normalizer.isNormalized(s, Normalizer.Form.NFC) && !FORBIDDEN_UNICODE_MAP.keySet().any{ s.contains(it) }
    }

    static String normalize(String s) {
        return Normalizer.normalize(s, Normalizer.Form.NFC).replace(FORBIDDEN_UNICODE_MAP)
    }

}
