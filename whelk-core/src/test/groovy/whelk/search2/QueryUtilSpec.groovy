package whelk.search2

import spock.lang.Specification

class QueryUtilSpec extends Specification {
    def "Encode URI slug"() {
        expect:
        QueryUtil.encodeUri("https://example/" + slug) == "https://example/" + encoded

        where:
        slug                        | encoded
        "Hästar"                    | "H%C3%A4star"
        "H%C3%A4star"               | "H%C3%A4star"
        "Försvunna personer"        | "F%C3%B6rsvunna%20personer"
        "F%C3%B6rsvunna%20personer" | "F%C3%B6rsvunna%20personer"
        "fnrblrghr1234567#it"       | "fnrblrghr1234567#it"
        "abc+abc"                   | "abc%2Babc"
        "abc%2Babc"                 | "abc%2Babc"
        "å/äö"                      | "%C3%A5/%C3%A4%C3%B6"
        "%C3%A5/%C3%A4%C3%B6"       | "%C3%A5/%C3%A4%C3%B6"
    }
}
