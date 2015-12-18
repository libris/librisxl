package whelk

import spock.lang.Specification

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
        1               | "old past"    | "10232k52"
        1449846940756   | "auth-1245"   | "lqsb00csj0b1pfpr"
        1449846940756   | "bib-245555"  | "lqsb00csj5mzw5hp"
        1449846940756   | "hold-11111"  | "lqsb00csj337qmh2"
        99999999999999  | "far future"  | "gcprhmd0d910qkbbj"
    }

    def "should generated time-based random id"() {
        expect:
        generator.generate() =~ /[bcdfghjklmnpqrstvwxz0-9]{16,}/
    }

}
