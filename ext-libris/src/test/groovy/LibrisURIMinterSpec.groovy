package se.kb.libris.whelks.plugin

import spock.lang.Specification
import groovy.util.logging.Slf4j as Log

//import org.codehaus.jackson.map.ObjectMapper
//import se.kb.libris.whelks.Document

@Log
class LibrisURIMinterSpec extends Specification {

    long PREDICTABLE_TIMESTAMP = Date.parse("yyyy-MM-dd", "2014-03-05").getTime()

    def "should base encode numbers"() {
        given:
        def minter = new LibrisURIMinter(alphabet: LibrisURIMinter.DEVOWELLED)
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

    @spock.lang.Ignore
    def "should construct path from component parts"() {
        given:
        def minter = new LibrisURIMinter(base: "//base/", timestampCaesarCipher: true)
        expect:
        minter.makePath("work", codes, keys) == uri
        where:
        codes               | keys                          | uri
        [31]                | ["Märk världen"]              | "work/21-mrkvrldn"
        []                  | ["Det"]                       | "work/det"
        [139409779957, 29]  | ["2012", "Där ute i mörkret"] | "work/flg72dq7-z-2012drtmrkrt"
    }

    def "should produce title based uri"() {
        given:
        def minter = new LibrisURIMinter(config)
        expect:
        minter.computePath(data, "auth") =~ uri
        where:
        data                                        | uri
        makeExample("Book", "Där ute i mörkret")    | '/work/[0-9b-z]+-\\w{2}-drtmrkrt'
    }

    def config = [
        base: "//base/",
        documentUriTemplate: "{+thing}?data",
        documentThingLink: "about",
        alphabet: "0123456789bcdfghjklmnpqrstvwxz",
        randomVariable: "randomKey",
        maxRandom: 899,
        timestampVariable: "timeKey",
        epochDate: "2014-01-01",
        timestampCaesarCipher: true,
        rulesByDataset: [
            "auth": [
                uriTemplate: "/{+basePath}/{timeKey}-{randomKey}-{compoundSlug}",
                ruleByBaseType: [
                    "CreativeWork": [
                        subclasses: ["Book"],
                        basePath: "work",
                        compoundSlugFrom: [[title: ["titleValue"]], "attributedTo"]
                    ]
                ]
            ]
        ]
    ]

    private def makeExample(type, title) {
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
