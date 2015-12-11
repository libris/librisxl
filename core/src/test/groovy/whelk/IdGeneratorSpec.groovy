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
        139409779957    | "auth-1245"   | "flg72dq7b1pfpr"
        139419779957    | "bib-245555"  | "rxs0q35k5mzw5h"
        139429779957    | "hold-11111"  | "384qdslw337qmh"
    }

    def "should generated time-based random id"() {
        expect:
        generator.generate() =~ /[bcdfghjklmnpqrstvxyz0-9]{14,}/
    }
}
