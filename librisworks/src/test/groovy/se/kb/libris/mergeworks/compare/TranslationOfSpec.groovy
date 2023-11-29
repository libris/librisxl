package se.kb.libris.mergeworks.compare

import se.kb.libris.mergeworks.Doc
import spock.lang.Specification
import whelk.Document

class TranslationOfSpec extends Specification {
    def "is compatible"() {
        given:
        def a = [:]
        def b = [:]
        if (typeA) a['@type'] = typeA
        if (typeB) b['@type'] = typeB
        if (langA) a['language'] = [['@id': 'https://id.kb.se/language/' + langA]]
        if (langB) b['language'] = [['@id': 'https://id.kb.se/language/' + langB]]
        if (titleA) a['hasTitle'] = [['@type': 'Title', 'mainTitle': titleA]]
        if (titleB) b['hasTitle'] = [['@type': 'Title', 'mainTitle': titleB]]

        expect:
        new TranslationOf().isCompatible(a, b) == result

        where:
        typeA  || typeB  || langA || langB || titleA || titleB || result
        'Work' || 'Work' || 'swe' || 'swe' || 'x'    || 'x'    || true
        'Work' || 'Text' || 'swe' || 'swe' || 'x'    || 'x'    || true
        'Work' || 'Work' || 'swe' || 'swe' || null   || 'x'    || true
        'Work' || 'Work' || 'swe' || 'swe' || null   || null   || true
        'Work' || 'Work' || 'swe' || 'fre' || null   || null   || false
        'Work' || 'Work' || 'swe' || null  || null   || null   || false
        'Work' || 'Work' || null  || null  || null   || null   || true
        'Work' || 'Work' || 'swe' || 'swe' || 'x'    || 'X.'   || true
        'Work' || 'Work' || 'swe' || 'swe' || 'x'    || 'y'    || false
    }
}
