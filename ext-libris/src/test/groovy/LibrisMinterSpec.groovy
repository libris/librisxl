package se.kb.libris.whelks.plugin

import spock.lang.Specification
import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper
import se.kb.libris.whelks.Document

@Log
class LibrisMinterSpec extends Specification {

    long PREDICTABLE_TIMESTAMP = Date.parse("yyyy-MM-dd", "2014-03-05").getTime()

    def "should produce title based uri"() {
        given:
        def minter = new LibrisMinter()
        expect:
        minter.mint(doc) == new URI(uri)
        where:
        doc                                             | uri
        buildBookDocumentWithTitle("Där ute i mörkret") | "/text/96ydww-zz8-drtmrkrt"
    }


    /*
     * Utility
     */
    Document buildBookDocumentWithTitle(title) {
        ObjectMapper mapper = new ObjectMapper()
        assert mapper
        def construct = ["about": [
                            "@type":"Book",
                            "title" : [
                                "@type" : "TitleEntity",
                                "titleValue" : title
                                ]
                            ]
                        ]
        return new Document()
                    .withData(mapper.writeValueAsString(construct))
                    .withContentType("application/ld+json")
                    .withTimestamp(PREDICTABLE_TIMESTAMP)
    }

}
