package whelk.util

import spock.lang.Specification

class JacksonSpec extends Specification {

    def "Serialize GString"() {
        given:
        def w = 'world'
        expect:
        Jackson.mapper.writeValueAsString("Hello, $w!") == '"Hello, world!"'
    }
}
