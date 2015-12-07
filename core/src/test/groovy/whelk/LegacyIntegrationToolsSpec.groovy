package whelk.util

import spock.lang.Specification

class LegacyIntegrationToolsSpec extends Specification {

    def tool = new LegacyIntegrationTools()

    def "should encode and crc32 hash identifier"() {
        expect:
        tool.generateId(data) == id
        where:
        data            | id
        "/auth/1"       | "2hq0kp111461"
        "/auth/12345"   | "jz6g15x6h3b1"
        "/bib/12345"    | "jz6g15x6h5f2"
        "/hold/12345"   | "jz6g15x6h1c3"
    }

}
