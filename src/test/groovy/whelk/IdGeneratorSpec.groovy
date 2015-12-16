package whelk

import spock.lang.Specification
import whelk.util.LegacyIntegrationTools

class IdGeneratorSpec extends Specification {

    def generator = new IdGenerator()

    def "should base encode numbers"() {
        expect:
        generator.baseEncode(n, caesared) == expected

        where:
        n               | expected      | caesared

        1               | "1"           | false
        31              | "11"          | false
        139409779957    | "6c70t5h7"    | false
        26938782        | "137pzd"      | false
        4175343705      | "5psqbrh"     | false
        3081193821      | "twgmzcp"     | true

        1               | "1"           | true
        30              | "10"          | true
        31              | "21"          | true
        139409779957    | "flg72dq7"    | true
        1008111600      | "1cgkg00"     | true
    }

    def "should encode and crc32 hash identifier"() {
        expect:
        generator.generate(n, data) == id
        where:
        n               | data          | id
        1449846940756   | "auth-1245"   | "lqsb00csjb1pfpr0"
        1449846940756   | "bib-245555"  | "lqsb00csj5mzw5hp"
        1449846940756   | "hold-11111"  | "lqsb00csj337qmh2"
        99999999999999  | "far future"  | "gcprhmd0d910qkbb"
    }

    def "should generated time-based random id"() {
        expect:
        generator.generate() =~ /[bcdfghjklmnpqrstvwxz0-9]{16,}/
    }

    def "should generate valid id based on legacy id"() {
        given:
        def id = LegacyIntegrationTools.generateId(legacyId)
        expect:
        id.length() == 15
        id.endsWith(endChar)
        where:
        legacyId          | endChar
        "/auth/123551211" | "1"
        "/bib/12312"      | "2"
        "/hold/999999999" | "3"

    }

    def "should generate new id exactly 16 chars long"() {
        given:
        def id = IdGenerator.generate()
        expect:
        id.length() == 16
    }
}
