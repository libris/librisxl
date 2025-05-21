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

    def "isSimple"() {
        expect:
        QueryUtil.isSimple(query) == result

        where:
        query                       | result
        "Hästar"                    | true
        "Häst*"                     | true
        "H*star"                    | false
        "*star"                     | false
        "Häst?"                     | true // treat these as no masking when last char. (e.g. pasted titles)
        "H?star"                    | false
        "H?star?"                   | false
        "H*star?"                   | false
        "?ästar"                    | false
        'Это дом'                   | true
        'Это д?м'                   | false
        'վիրված'                    | true
        'վիրվ?ած'                   | false
    }
}
