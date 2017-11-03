package whelk.converter.marc

import spock.lang.*


@Unroll
class MarcFrameConverterUtilsSpec extends Specification {

    def "should extract #token from #uri"() {
        expect:
        MarcSimpleFieldHandler.extractToken(tplt, uri) == token
        where:
        tplt            | uri               | token
        "/item/{_}"     | "/item/thing"     | "thing"
        "/item/{_}/eng" | "/item/thing/eng" | "thing"
        "/item/{_}/swe" | "/item/thing/eng" | null
    }

    def "should parse formatted #date"() {
        expect:
        MarcSimpleFieldHandler.parseDate(date)
        where:
        date << ['2014-06-10T12:05:05.0+02:00', '2014-06-10T12:05:05.0+0200']
    }

    def "should treat arrays as sets of objects"() {
        given:
        def obj = [:]
        def prop = "label"
        when:
        2.times {
            BaseMarcFieldHandler.addValue(obj, prop, value, true)
        }
        then:
        obj[prop] == [value]
        where:
        value << ["Text", ["@id": "/link"]]
    }

    def "should get as list"() {
        expect:
        Util.asList('1') == ['1']
        Util.asList(['1']) == ['1']
        Util.asList('') == ['']
        Util.asList(null) == []
    }

    def "should get by path"() {
        expect:
        Util.getAllByPath(entity, path) == values
        where:
        entity                          | path              | values
        [key: '1']                      | 'key'             | ['1']
        [key: ['1', '2']]               | 'key'             | ['1', '2']
        [item: [[key: '1']]]            | 'item.key'        | ['1']
        [item: [[key: '1'],
                [key: '2']]]            | 'item.key'        | ['1', '2']
        [part: [[item: [[key: '1']]],
                [item: [key: '2']]]]    | 'part.item.key'   | ['1', '2']
    }

    def "should process includes"() {
        expect:
        MarcRuleSet.processInclude([patterns: [a: [a:1]]], [include: 'a', b:2]) == [a:1, b:2]
    }

}
