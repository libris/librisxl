package se.kb.libris.mergeworks.compare

import spock.lang.Specification

class ContentTypeSpec extends Specification {
    def "is compatible"() {
        expect:
        new ContentType().isCompatible(a, b) == result

        where:
        a                                                 || b                                           || result
        [['@id': 'https://id.kb.se/term/rda/Text']]       || [['@id': 'https://id.kb.se/term/rda/Text']] || true
        [['@id': 'https://id.kb.se/term/rda/StillImage']] || [['@id': 'https://id.kb.se/term/rda/Text']] || true
        []                                                || [['@id': 'https://id.kb.se/term/rda/Text']] || true
        [['@id': 'https://id.kb.se/term/rda/X']]          || [['@id': 'https://id.kb.se/term/rda/Text']] || false
        [['@id': 'https://id.kb.se/term/rda/X']]          || []                                          || false
        [['label': 'x']]                                  || [['@id': 'https://id.kb.se/term/rda/Text']] || false
    }
}
