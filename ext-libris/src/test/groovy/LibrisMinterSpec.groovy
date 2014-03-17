package se.kb.libris.whelks.plugin

import spock.lang.Specification
import spock.lang.Shared
import groovy.util.logging.Slf4j as Log

//import org.codehaus.jackson.map.ObjectMapper
//import se.kb.libris.whelks.Document

@Log
class LibrisMinterSpec extends Specification {

    long PREDICTABLE_TIMESTAMP = Date.parse("yyyy-MM-dd", "2014-03-05").getTime()

    def "should base encode numbers"() {
        given:
        def minter = new LibrisMinter()
        expect:
        minter.baseEncode(n, caesared) == expected

        where:
        n               | expected      | caesared

        1               | "1"           | false
        31              | "11"          | false
        139409779957    | "6c70t5h7"    | false

        1               | "1"           | true
        30              | "10"          | true
        31              | "21"          | true
        139409779957    | "flg72dq7"    | true
    }

    def "should construct path from component parts"() {
        given:
        def minter = new LibrisMinter("//base/", null, null, true)
        expect:
        minter.makePath("Book", codes, keys) == uri
        where:
        codes               | keys                          | uri
        [31]                | ["Märk världen"]              | "book/21-mrkvrldn"
        []                  | ["Det"]                       | "book/det"
        [139409779957, 29]  | ["2012", "Där ute i mörkret"] | "book/flg72dq7-z-2012drtmrkrt"
    }

    def "should produce title based uri"() {
        given:
        def typeRules = [about: ['@type': true, title: [titleValue: true]]]
        def minter = new LibrisMinter("//base/", typeRules, "2014-01-01", true)
        expect:
        minter.computePath(data) =~ uri
        where:
        data                                    | uri
        newData("Book", "Där ute i mörkret")    | 'book/[0-9b-z]+-drtmrkrt'
    }

    private def newData(type, title) {
        return [
            "about": [
                "@type": type,
                "title": ["titleValue": title]
            ]
        ]
    }

    /*
    private def newDoc(data) {
        ObjectMapper mapper = new ObjectMapper()
        return new Document()
                    .withData(mapper.writeValueAsString(data))
                    .withContentType("application/ld+json")
                    .withTimestamp(PREDICTABLE_TIMESTAMP)
    }
    */

}
