package whelk.util

import spock.lang.Specification

class UnicodeSpec extends Specification {

    def "normalize to NFC"() {
        given:
        String s = "Adam f*å*r en melding fra det gamle friidrettslaget, som ønsker at"
        String nfc = "Adam f*å*r en melding fra det gamle friidrettslaget, som ønsker at"
        expect:
        Unicode.isNormalized(s) == false
        Unicode.normalize(s) == nfc
    }

    def "normalize typographic ligatures"() {
        given:
        String s = "societal bene*ﬁ*t is maximized. This means that the tracks should be used by as much tra*ﬃ*c"
        String norm = "societal bene*fi*t is maximized. This means that the tracks should be used by as much tra*ffi*c"
        expect:
        Unicode.isNormalized(s) == false
        Unicode.normalize(s) == norm
    }

    def "strip BOM"() {
        given:
        String s = "9th Koli Calling International Conference on Computing Education Research\ufeff, October 29–November 1, 2009"
        String norm = "9th Koli Calling International Conference on Computing Education Research, October 29–November 1, 2009"
        expect:
        Unicode.isNormalized(s) == false
        Unicode.normalize(s) == norm
    }

    def "trim noise"() {
        expect:
        Unicode.trimNoise(dirty) == clean
        where:
        dirty                                    | clean
        ' _.:;|{[Überzetsung]}|;:. '             | 'Überzetsung'
        ' _.:;|(Überzetsung)|;:. '               | '(Überzetsung)'
        ' _.:;| Ü b e r - z e t - s u n g |;:. ' | 'Ü b e r - z e t - s u n g'
    }

    def "trim"() {
        expect:
        Unicode.trim(dirty) == clean
        where:
        dirty                               | clean
        ' SPACE '                           | 'SPACE'
        '\u00A0NO-BREAK SPACE\u00A0'        | 'NO-BREAK SPACE'
        '\u202FNARROW NO-BREAK SPACE\u202F' | 'NARROW NO-BREAK SPACE'
        '\u2007FIGURE SPACE\u2007'          | 'FIGURE SPACE'
        '\u2060WORD JOINER\u2060'           | 'WORD JOINER'
        'a\u00A0\u202F\u2007\u2060b'        | 'a\u00A0\u202F\u2007\u2060b'
    }
}
