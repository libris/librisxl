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

    def "trim noise()"() {
        expect:
        Unicode.trimNoise(dirty) == clean
        where:
        dirty                                    | clean
        ' _.:;|{[Überzetsung]}|;:. '             | 'Überzetsung'
        ' _.:;|(Überzetsung)|;:. '               | '(Überzetsung)'
        ' _.:;| Ü b e r - z e t - s u n g |;:. ' | 'Ü b e r - z e t - s u n g'
    }

    def "u"() {
        given:
        String s = "übers"   //uU+CC88
        String nfc = "übers" //U+C3BC
        expect:
        Unicode.isNormalized(s) == false
        Unicode.normalize(s) == nfc
    }

    def "asciiFold"() {
        expect:
        Unicode.asciiFold(unicode) == ascii

        where:
        unicode     | ascii
        'Désidéria' | 'Desideria'
    }
}
