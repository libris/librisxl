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

    def "should generate valid id based on legacy id"() {
        given:
        def id = tool.generateId(legacyId)
        expect:
        id.length() == 15
        id.endsWith(endChar)
        where:
        legacyId          | endChar
        "/auth/123551211" | "1"
        "/bib/12312"      | "2"
        "/hold/999999999" | "3"
    }

}
