package whelk.util

import spock.lang.Specification

class LegacyIntegrationToolsSpec extends Specification {

    def tool = new LegacyIntegrationTools()

    def "should encode and crc32 hash identifier"() {
        expect:
        tool.generateId(data) == id
        where:
        data                | id
        "/auth/1"           | "hftwp11146kkhc1"
        "/auth/12345"       | "zw9c5x6h3bj0c87"
        "/bib/12345"        | "5ng15x6h05ft7lr"
        "/hold/12345"       | "bgmp5x6h1ct51qd"
        "/auth/123551211"   | "53nnvnsp2gq1qpz"
        "/hold/999999999"   | "59snjbd943p7s0x"
    }

}
