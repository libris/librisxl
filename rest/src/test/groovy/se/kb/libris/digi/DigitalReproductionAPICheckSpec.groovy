package se.kb.libris.digi

import spock.lang.Specification
import spock.lang.Unroll

import static se.kb.libris.digi.DigitalReproductionAPI.Type.ARRAY
import static se.kb.libris.digi.DigitalReproductionAPI.Type.STRING

class DigitalReproductionAPICheckSpec extends Specification {

    @Unroll
    def "check passes for #desc"() {
        expect:
        DigitalReproductionAPI.check(thing, path, expected) == null

        where:
        desc                | thing            | path       | expected
        'matching value'    | ['a': 'b']       | ['a']      | 'b'
        'matching STRING'   | ['a': 'b']       | ['a']      | STRING
        'matching ARRAY'    | ['a': ['x']]     | ['a']      | ARRAY
        'nested value'      | ['a': ['b': 'c']] | ['a', 'b'] | 'c'
    }

    @Unroll
    def "check throws badRequest for #desc"() {
        when:
        DigitalReproductionAPI.check(thing, path, expected)

        then:
        def e = thrown(RequestException)
        e.code == 400
        e.msg.startsWith('Expected')

        where:
        desc               | thing        | path      | expected
        'missing key'      | [:]          | ['a']     | 'X'
        'wrong value'      | ['a': 'b']   | ['a']     | 'X'
        'wrong type'       | ['a': 'b']   | ['a']     | ARRAY
        'nested missing'   | ['a': [:]]   | ['a', 'b'] | STRING
    }

    def "check with a null expected does not NPE (Groovy == was null-safe)"() {
        when: 'a present value is compared against null expected'
        DigitalReproductionAPI.check(['a': 'b'], ['a'], null)

        then: 'a clean badRequest, not a NullPointerException'
        def e = thrown(RequestException)
        e.code == 400

        and: 'and null == null (absent key) passes rather than throwing'
        DigitalReproductionAPI.check(['a': 'b'], ['missing'], null) == null
    }

    def "check error message reports <MISSING> for an absent value"() {
        when:
        DigitalReproductionAPI.check([:], ['a'], 'X')

        then:
        def e = thrown(RequestException)
        e.msg == 'Expected X at [a], got: <MISSING>'
    }
}
