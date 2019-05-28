package whelk.rest.api

import spock.lang.Specification

import whelk.rest.api.ESQueryLensBoost
import whelk.JsonLd

class ESQueryLensBoostSpec extends Specification {

    def "should compute boost fields from lenses"() {
        given:
        def ns = 'http://example.org/ns/'

        def context = [
            '@context': [
                '@vocab': ns
            ]
        ]

        def display = [
            "lensGroups": [
                "chips": [
                    "lenses": [
                        "Instance": [
                            "classLensDomain": "Instance",
                            "showProperties": ["hasTitle", "comment"]
                        ],
                        "Publication": [
                            "classLensDomain": "Publication",
                            "showProperties": ["agent"]
                        ]
                    ]
                ],
                "cards": [
                    "lenses": [
                        "Instance": [
                            "classLensDomain": "Instance",
                            "showProperties": ["hasTitle", "publication"]
                        ]
                    ]
                ]
            ]
        ]

        def vocab = [
            "@graph": [
                ["@id": "${ns}QualifiedRole", "@type": "Class"],
                ["@id": "${ns}Publication", "@type": "Class",
                 "subClassOf": ["@id": "${ns}QualifiedRole"]],
                ["@id": "${ns}Title", "@type": "Class",
                 "subClassOf": ["@id": "${ns}StructuredValue"]],
                ["@id": "${ns}hasTitle", "@type": "ObjectProperty",
                 "range": [["@id": "${ns}Title"]]],
                ["@id": "${ns}publication", "@type": "ObjectProperty",
                 "range": [["@id": "${ns}Publication"]]],
                ["@id": "${ns}agent", "@type": "ObjectProperty",
                 "range": [["@id": "${ns}Agent"]]],
            ]
        ]

        def jsonld = new JsonLd(context, display, vocab)
        def lensBoost = new ESQueryLensBoost(jsonld)

        when:
        def boostFields = lensBoost.computeBoostFieldsFromLenses(["Instance"] as String[])

        then:
        boostFields == ['_str^100', 'hasTitle._str^200', 'comment^200', 'publication.agent._str^10']
    }

}
