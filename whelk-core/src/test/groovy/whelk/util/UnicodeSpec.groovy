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
        dirty                                                       | clean
        ' SPACE '                                                   | 'SPACE'
        '\u00A0\u00A0\u00A0NO-BREAK SPACE\u00A0\u00A0\u00A0'        | 'NO-BREAK SPACE'
        '\u202F\u202F\u202FNARROW NO-BREAK SPACE\u202F\u202F\u202F' | 'NARROW NO-BREAK SPACE'
        '\u2007\u2007\u2007FIGURE SPACE\u2007\u2007\u2007'          | 'FIGURE SPACE'
        '\u2060\u2060\u2060WORD JOINER\u2060\u2060\u2060'           | 'WORD JOINER'
        'keep\u00A0\u202F\u2007\u2060us'                            | 'keep\u00A0\u202F\u2007\u2060us'
        'LINE FEED\n\n\n'                                           | 'LINE FEED'
        'CARRIAGE RETURN\r\r\r'                                     | 'CARRIAGE RETURN'
        'line breaks\u000A\u000B\u000C\u000D\u0085\u2028\u2029'     | 'line breaks'
        '\r\nkeep leading line breaks'                              | '\r\nkeep leading line breaks'
        
    }

    def "stripPrefix"() {
        expect:
        Unicode.stripPrefix(s, prefix) == result
        where:
        s     | prefix || result
        ""    | ""     || ""
        "sss" | "ss"   || "s"
        "sss" | "xx"   || "sss"
        ""    | "xx"   || ""
        "sss" | ""     || "sss"
    }

    def "stripSuffix"() {
        expect:
        Unicode.stripSuffix(s, suffix) == result
        where:
        s     | suffix || result
        ""    | ""     || ""
        ""    | "s"    || ""
        "sss" | ""     || "sss"
        "sss" | "sss"  || ""
        "sss" | "ss"   || "s"
        "sss" | "s"    || "ss"
        "sss" | "xx"   || "sss"
    }

    def "double quotation marks"() {
        expect:
        Unicode.isNormalizedDoubleQuotes(dirty) == (dirty == clean)
        Unicode.normalizeDoubleQuotes(dirty) == clean
        where:
        dirty                                                       | clean
        '"my query"'                                                | '"my query"'
        '”my query”'                                                | '"my query"'
        '“my query”'                                                | '"my query"'
        'this is ”my query” string'                                 | 'this is "my query" string'
        'this is “my query” string'                                 | 'this is "my query" string'
    }
    
    def "guess unicode script"() {
        expect:
        Unicode.guessScript(string) == script
        where:
        script                                        | string
        Optional.empty()                              | ''
        Optional.empty()                              | '  '
        Optional.of(Character.UnicodeScript.CYRILLIC) | 'Это дом'
        Optional.of(Character.UnicodeScript.LATIN)    | 'dom'
        Optional.of(Character.UnicodeScript.ARMENIAN) | 'վիրված'
        Optional.of(Character.UnicodeScript.ARABIC)   | 'می خوانم و غرق در کویر می شوم'
        Optional.of(Character.UnicodeScript.HIRAGANA) | 'とんとんとんと'
    }

    def "guess 15924 script"() {
        expect:
        Unicode.guessIso15924ScriptCode(string) == script
        where:
        script              | string
        Optional.empty()    | ''
        Optional.empty()    | '  '
        Optional.of('Cyrl') | 'Это дом'
        Optional.of('Arab') | 'داستان یک سفر'
        Optional.of('Arab') | 'پشمالو'
        Optional.of('Armn') | 'Պիպին նավի վրա'
        Optional.of('Kana') | 'デスノート'
        Optional.of('Hira') | 'とんとんとんと'
    }

    def "rtl"() {
        expect:
        Unicode.guessScript(string).map(Unicode::isRtl).orElse(false) == rtl
        where:
        string                          | rtl
        ''                              | false
        '  '                            | false
        'Это дом'                       | false
        'dom'                           | false
        'վիրված'                        | false
        'می خوانم و غرق در کویر می شوم' | true
        'קונסט און קינסטלער'            | true
    }
        
    def "u"() {
        given:
        String s = "übers"   //uU+CC88
        String nfc = "übers" //U+C3BC
        expect:
        Unicode.isNormalized(s) == false
        Unicode.normalize(s) == nfc
    }

    def "removeAllDiacritics"() {
        expect:
        Unicode.removeAllDiacritics(in) == out

        where:
        in               | out
        'Désidéria'      | 'Desideria'
        'Антон Павлович' | 'Антон Павлович'
        'Åkerbärsfrön'   | 'Akerbarsfron'
    }

    def "removeDiacritics"() {
        expect:
        Unicode.removeDiacritics(in) == out

        where:
        in               | out
        'Désidéria'      | 'Desideria'
        'Антон Павлович' | 'Антон Павлович'
        'Åkerbärsfrön'   | 'Åkerbärsfrön'
    }

    def "levenshtein"() {
        expect:
        Unicode.levenshteinDistance(a, b) == distance

        where:
        a        | b         || distance
        ''       | ''        || 0
        'abc'    | 'abc'     || 0
        'ab'     | 'abc'     || 1
        'abc'    | 'ab'      || 1
        'abc'    | 'abd'     || 1
        'acb'    | 'abc'     || 2
        'kitten' | 'sitting' || 3
        'abc'    | '1234567' || 7
    }

    def "damerauLevenshtein"() {
        expect:
        Unicode.damerauLevenshteinDistance(a, b) == distance

        where:
        a        | b         || distance
        ''       | ''        || 0
        'abc'    | 'abc'     || 0
        'ab'     | 'abc'     || 1
        'abc'    | 'ab'      || 1
        'acb'    | 'abc'     || 1
        'acb'    | 'abc'     || 1
        'kitten' | 'sitting' || 3
        '124356' | '123456'  || 1
        '143256' | '123456'  || 2
        'abc'    | '1234567' || 7
    }
}