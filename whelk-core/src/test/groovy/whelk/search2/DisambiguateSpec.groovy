package whelk.search2

import spock.lang.Specification
import whelk.JsonLd
import whelk.Whelk
import whelk.search.QueryDateTime
import whelk.search2.querytree.Any
import whelk.search2.querytree.DateTime
import whelk.search2.querytree.InvalidValue
import whelk.search2.querytree.Key
import whelk.search2.querytree.Link
import whelk.search2.querytree.Numeric
import whelk.search2.querytree.Path
import whelk.search2.querytree.Property
import whelk.search2.querytree.TestData
import whelk.search2.querytree.Token
import whelk.search2.querytree.VocabTerm
import whelk.search2.querytree.YearRange

class DisambiguateSpec extends Specification {
    static Disambiguate disambiguate = TestData.getDisambiguate()
    static JsonLd jsonLd = TestData.getJsonLd()

    def "try map string to property or other recognized key"() {
        expect:
        disambiguate.mapQueryKey(new Token.Raw(s)) == result

        where:
        s                   | result
        'p1'                | new Property('p1', jsonLd)
        'p1Label'           | new Property('p1', jsonLd)
        'unrecognizedLabel' | new Key.UnrecognizedKey(new Token.Raw('unrecognizedLabel'))
        '@id'               | new Key.RecognizedKey(new Token.Raw('@id'))
        '_str'              | new Key.RecognizedKey(new Token.Raw('_str'))
        'p'                 | new Property('p', jsonLd)
        'pLabel'            | new Property('p2', jsonLd)
        'pp'                | new Key.AmbiguousKey(new Token.Raw('pp'))
        'p3.p4'             | new Path(List.of(new Property('p3', jsonLd), new Property('p4', jsonLd)))
    }

    def "try map string to a recognized value type for the associated property"() {
        given:
        def res = disambiguate.mapValueForProperty(new Property(p, jsonLd), v)

        expect:
        res == Optional.ofNullable(result)

        where:
        p          | v                                       | result
        'p1'       | '*'                                     | new Any.Wildcard()
        'p2'       | '*'                                     | new Any.Wildcard()
        'p3'       | '*'                                     | new Any.Wildcard()
        'rdf:type' | 'T1'                                    | new VocabTerm('T1', [:])
        'rdf:type' | 'not a class'                           | InvalidValue.forbidden('not a class')
        'rdf:type' | 't'                                     | new VocabTerm('T', [:])
        'rdf:type' | 'tt'                                    | InvalidValue.ambiguous('tt')
        'p2'       | 'E1'                                    | new VocabTerm('E1', [:])
        'p1'       | 'v'                                     | null
        'p2'       | 'v'                                     | InvalidValue.forbidden('v')
        'p3'       | 'v'                                     | null
        'p1'       | 'sao:H%C3%A4star'                       | null
        'p1'       | 'https://id.kb.se/term/sao/H%C3%A4star' | null
        'p3'       | 'sao:H%C3%A4star'                       | new Link('https://id.kb.se/term/sao/H%C3%A4star')
        'p3'       | 'https://id.kb.se/term/sao/H%C3%A4star' | new Link('https://id.kb.se/term/sao/H%C3%A4star')
        'p1'       | '1990'                                  | new Numeric(1990)
        'p1'       | '90-talet'                              | null
        'p1'       | '1990-2000'                             | new YearRange('1990', '2000', null)
        'p1'       | '1990-'                                 | new YearRange('1990', "", null)
        'p1'       | '-1990'                                 | new YearRange("", '1990', null)
        'p2'       | '1990'                                  | InvalidValue.forbidden('1990')
        'p2'       | '1990-2000'                             | InvalidValue.forbidden('1990-2000')
        'p3'       | '1990-2000'                             | null
        'p12'      | '1990'                                  | new DateTime(QueryDateTime.parse('1990'))
        'p12'      | '1990-01'                               | new DateTime(QueryDateTime.parse('1990-01'))
        'p12'      | '1990-01-01'                            | new DateTime(QueryDateTime.parse('1990-01-01'))
        'p12'      | '1990-2000'                             | new YearRange('1990', '2000', null)
        'p12'      | '1990-'                                 | new YearRange('1990', "", null)
        'p12'      | '-1990'                                 | new YearRange("", '1990', null)
        'p12'      | 'xyz'                                   | null
        'p12'      | '19900101'                              | null
        'p12'      | '1990/01/01'                            | null
    }
}
