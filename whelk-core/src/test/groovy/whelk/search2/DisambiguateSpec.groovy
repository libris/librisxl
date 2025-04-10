package whelk.search2

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.querytree.InvalidValue
import whelk.search2.querytree.Key
import whelk.search2.querytree.Link
import whelk.search2.querytree.Literal
import whelk.search2.querytree.Property
import whelk.search2.querytree.TestData
import whelk.search2.querytree.VocabTerm

class DisambiguateSpec extends Specification {
    static Disambiguate disambiguate = TestData.getDisambiguate()
    static JsonLd jsonLd = TestData.getJsonLd()

    def "try map string to property or other recognized key"() {
        expect:
        disambiguate.mapKey(s) == result

        where:
        s                   | result
        'p1'                | new Property('p1', jsonLd)
        'p1Label'           | new Property('p1', jsonLd)
        'unrecognizedLabel' | new Key.UnrecognizedKey('unrecognizedLabel')
        '@id'               | new Key.RecognizedKey('@id')
        '_str'              | new Key.RecognizedKey('_str')
        'p'                 | new Property('p', jsonLd)
        'pLabel'            | new Property('p2', jsonLd)
        'pp'                | new Key.AmbiguousKey('pp')
    }

    def "get value for property"() {
        expect:
        disambiguate.getValueForProperty(new Property(p, jsonLd), v) == result

        where:
        p          | v                                       | result
        'p1'       | '*'                                     | new Literal('*')
        'p2'       | '*'                                     | new Literal('*')
        'p3'       | '*'                                     | new Literal('*')
        'rdf:type' | 'T1'                                    | new VocabTerm('T1', [:])
        'rdf:type' | 'not a class'                           | new InvalidValue.ForbiddenValue('not a class')
        'rdf:type' | 't'                                     | new VocabTerm('T', [:])
        'rdf:type' | 'tt'                                    | new InvalidValue.AmbiguousValue('tt')
        'p2'       | 'E1'                                    | new VocabTerm('E1', [:])
        'p1'       | 'v'                                     | new Literal('v')
        'p2'       | 'v'                                     | new InvalidValue.ForbiddenValue('v')
        'p3'       | 'v'                                     | new Literal('v')
        'p1'       | 'sao:H%C3%A4star'                       | new Literal('sao:H%C3%A4star')
        'p1'       | 'https://id.kb.se/term/sao/H%C3%A4star' | new Literal('https://id.kb.se/term/sao/H%C3%A4star')
        'p3'       | 'sao:H%C3%A4star'                       | new Link('https://id.kb.se/term/sao/H%C3%A4star')
        'p3'       | 'https://id.kb.se/term/sao/H%C3%A4star' | new Link('https://id.kb.se/term/sao/H%C3%A4star')
    }
}
