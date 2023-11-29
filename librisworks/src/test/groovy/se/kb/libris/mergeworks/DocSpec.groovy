package se.kb.libris.mergeworks

import whelk.Document
import whelk.Whelk

import spock.lang.Specification

class DocSpec extends Specification {
    static def whelk = null
    static {
        try {
            whelk = Whelk.createLoadedSearchWhelk()
        } catch (Exception e) {
            System.err.println("Unable to instantiate whelk: $e")
        }
    }

    def "work has multiple parts"() {
        given:
        Map data = ['@graph': [[:], mainEntity]]
        Doc doc = new Doc(whelk, new Document(data))

        expect:
        doc.hasParts() == result

        where:
        mainEntity                                                                                                         || result
        ['instanceOf': ['hasPart': [['@type': 'Text']]]]                                                                   || true
        ['hasTitle': [['@type': 'Title', 'hasPart': [[:], [:]]]], 'instanceOf': ['@type': 'Text']]                         || true
        ['hasTitle': [['@type': 'Title', 'hasPart': [[:]]]], 'instanceOf': ['@type': 'Text']]                              || false
        ['hasTitle': [['@type': 'Title', 'hasPart': [['partNumber': ['1', '2']]]]], 'instanceOf': ['@type': 'Text']]       || true
        ['hasTitle': [['@type': 'Title', 'hasPart': [['partNumber': ['1']]]]], 'instanceOf': ['@type': 'Text']]            || false
        ['hasTitle': [['@type': 'Title', 'mainTitle': 'x ; y']], 'instanceOf': ['@type': 'Text']]                          || true
        ['hasTitle': [['@type': 'Title', 'mainTitle': 'x / y ; z']], 'instanceOf': ['@type': 'Text']]                      || false
        ['hasTitle': [['@type': 'Title', 'mainTitle': 'x ;', 'subtitle': 'y']], 'instanceOf': ['@type': 'Text']]           || true
        ['hasTitle': [['@type': 'Title', 'mainTitle': 'x / y ;', 'titleRemainder': 'z']], 'instanceOf': ['@type': 'Text']] || false
    }

    def "work is not regular text"() {
        given:
        Map data = ['@graph': [[:], mainEntity]]
        Doc doc = new Doc(whelk, new Document(data))

        expect:
        doc.isNotRegularText() == result

        where:
        mainEntity                                                                                                        || result
        ['instanceOf': ['@type': 'Text']]                                                                                 || false
        ['instanceOf': ['@type': 'Text', 'contentType': [['@id': 'https://id.kb.se/term/rda/TactileText']]]]              || true
        ['carrierType': [['@id': 'https://id.kb.se/marc/Braille']], 'instanceOf': ['@type': 'Text']]                      || true
        ['instanceOf': ['@type': 'Text', 'genreForm': [['@id': 'https://id.kb.se/term/barngf/Mekaniska%20b%C3%B6cker']]]] || true
        ['instanceOf': ['@type': 'Text', 'genreForm': [['@id': 'https://id.kb.se/term/saogf/Taktila%20verk']]]]           || true
        ['instanceOf': ['@type': 'Text', 'genreForm': [['@id': 'https://id.kb.se/term/saogf/Punktskriftsb%C3%B6cker']]]]  || true
    }

    def "parse extent"() {
        expect:
        Doc.numPages(extent) == pages

        where:
        extent                                    | pages
        ""                                        | -1
        "114, [1] s."                             | 114
        "[4], 105, [2] s."                        | 105
        "21 s., ([4], 21, [5] s.)"                | 21
        "[108] s., (Ca 110 s.)"                   | 110
        "80 s., (80, [3] s., [8] pl.-bl. i färg)" | 80
        "622, [8] s."                             | 622
        "[2] s., s. 635-919, [7] s."              | 919 // ??
        "[1], iv, 295 s."                         | 295
        "3 vol."                                  | -1
        //"249, (1) s."                             | 249
        //"[8] s., s. 11-370"                       | 370
        //[12] s., s. 15-256                        | 256
        "25 onumrerade sidor"                     | 25
    }
}
