package se.kb.libris.whelks.plugin

import spock.lang.Specification
import spock.lang.Shared
import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper
import se.kb.libris.whelks.Document

@Log
class LibrisMinterSpec extends Specification {

    long PREDICTABLE_TIMESTAMP = Date.parse("yyyy-MM-dd", "2014-03-05").getTime()

    @Shared minter = new LibrisMinter("//base/")

    def "should construct path from component parts"() {
        expect:
        minter.makePath("Book", codes, keys) == uri
        where:
        codes               | keys                          | uri
        [31]                | ["Märk världen"]              | "book/11-mrkvrldn"
        []                  | ["Det"]                       | "book/det"
        [139409779957, 29]  | ["2012", "Där ute i mörkret"] | "book/6c70t5h7-z-2012drtmrkrt"
    }

    /*
    def "should produce title based uri"() {
        expect:
        minter.mint(doc) == new URI(uri)
        where:
        doc                                 | uri
        newDoc("Book", "Där ute i mörkret") | "/text/96ydww-zz8-drtmrkrt"
    }

    def newDoc(type, title) {
        ObjectMapper mapper = new ObjectMapper()
        assert mapper
        def construct = ["about": ["@type": type,
                            "title": ["titleValue": title]]]
        return new Document()
                    .withData(mapper.writeValueAsString(construct))
                    .withContentType("application/ld+json")
                    .withTimestamp(PREDICTABLE_TIMESTAMP)
    }
    */

}
