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

    def "should scramble slug"() {
        given:
        def minter = new LibrisURIMinter(alphabet: "0123456789bcdfghjklmnpqrstvwxz")
        expect:
        minter.scramble(value) == slug
        where:
        value                   | slug
        "Märk världen"          | "mrkvrldn"
        "Det"                   | "det"
        "Där ute i mörkret"     | "drtmrkrt"
    }

    def "should compute path from data using variables and compound keys"() {
        given:
        def minter = new LibrisURIMinter(config)
        minter.metaClass.createRandom = { 898 }
        minter.metaClass.createTimestamp = { 139409779957 }
        expect:
        minter.computePath(data, "auth") == uri
        where:
        data                                        | uri
        doc("@type": "Book",
            title: [
                titleValue: "Där ute i mörkret"],
            publicationYear: "2012")                | '/work/flg72dq7-zx-drtmrkrt2012'
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
                        compoundSlugFrom: [[title: ["titleValue"]], "publicationYear", "attributedTo"]
                    ]
                ]
            ]
        ]
    ]

    private def doc(thing) {
        return ["about": thing]
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
