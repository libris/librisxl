package whelk

import spock.lang.Specification
import whelk.URIMinter

class URIMinterSpec extends Specification {

    def "should scramble slug"() {
        given:
        def minter = new URIMinter(
                alphabet: "0123456789bcdfghjklmnpqrstvwxz",
                slugCharInAlphabet: true,
                minSlugSize: 3)
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
        def minter = new URIMinter(config)
        minter.metaClass.createRandom = { 898 }
        minter.metaClass.createTimestamp = { 139409779957 }
        expect:
        minter.computePath(data, "auth") == uri
        where:
        data                                        | uri
        ["@type": "Book",
            instanceTitle: [
                titleValue: "Där ute i mörkret"],
            publicationYear: "2012"]                | '/work/flg72dq7-zx-drtmrkrt2012'
        ["@type": "Record", identifier: "123"]      | '/record/123'
    }

    def config = [
        base: "//base/",
        documentUriTemplate: "{+thing}?data",
        objectLink: "about",
        alphabet: "0123456789bcdfghjklmnpqrstvwxz",
        randomVariable: "randomKey",
        maxRandom: 899,
        timestampVariable: "timeKey",
        epochDate: "2014-01-01",
        timestampCaesarCipher: true,
        slugCharInAlphabet: true,
        rulesByDataset: [
            "auth": [
                uriTemplate: "/{+basePath}/{timeKey}-{randomKey}-{compoundSlug}",
                ruleByBaseType: [
                    "CreativeWork": [
                        subclasses: ["Book"],
                        basePath: "work",
                        compoundSlugFrom: [[instanceTitle: ["titleValue"]], "publicationYear", "attributedTo"]
                    ],
                    "Record": [
                        "uriTemplate": "/record/{identifier}",
                        "variables": ["identifier"]
                    ]
                ]
            ]
        ]
    ]

}
