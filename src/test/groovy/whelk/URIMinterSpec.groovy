package whelk

import spock.lang.Specification
import whelk.URIMinter


class URIMinterSpec extends Specification {

    def "should base encode numbers"() {
        given:
        def minter = new URIMinter()
        expect:
        minter.baseEncode(n, caesared) == expected

        where:
        n               | expected      | caesared

        1               | "1"           | false
        31              | "11"          | false
        139409779957    | "6c70t5h7"    | false
        26938782        | "137pzd"      | false
        4175343705      | "5psqbrh"     | false
        3081193821      | "twgmzcp"     | true

        1               | "1"           | true
        30              | "10"          | true
        31              | "21"          | true
        139409779957    | "flg72dq7"    | true
        1008111600      | "1cgkg00"     | true
    }

    def "should encode and crc32 hash identifier"() {
        given:
        def minter = new URIMinter()
        expect:
        minter.mint(n, seed) == id
        where:
        n              | seed         | id
        139409779957   | "auth-1245"  | "/flg72dq7b1pfpr"
        139419779957   | "bib-245555" | "/rxs0q35k5mzw5h"
        139429779957   | "hold-11111" | "/384qdslw337qmh"
        //139439779957   | null         | "flg72dq7"
    }


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
