package whelk.util

import spock.lang.Specification

class LegacyIntegrationToolsSpec extends Specification {

    def tool = new LegacyIntegrationTools()

    def "should encode and crc32 hash identifier"() {
        expect:
        tool.generateId(data) == id
        where:
        data            | id
        "/auth/1"       | "2hq0kp11146kkh1"
        "/auth/12345"   | "jz6g15x6h3bj0c1"
        "/bib/12345"    | "jz6g15x6h5ft7l2"
        "/hold/12345"   | "jz6g15x6h1ct513"
    }

}
