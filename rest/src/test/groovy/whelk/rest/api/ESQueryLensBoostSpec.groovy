package whelk.rest.api

import spock.lang.Specification

import whelk.rest.api.ESQueryLensBoost
import whelk.JsonLd

class ESQueryLensBoostSpec extends Specification {

    def "should compute boost fields from lenses"() {
        given:
        def context = [
            '@context': [
                '@vocab': 'http://example.org/ns/'
            ]
        ]

        def display = [
            "lensGroups": [
                "chips": [
                    "lenses": [
                        "Instance": [
                            "showProperties": ["hasTitle", "comment"]
                        ],
                        "Publication": [
                            "showProperties": ["agent"]
                        ]
                    ]
                ],
                "cards": [
                    "lenses": [
                        "Instance": [
                            "showProperties": ["hasTitle", "publication"]
                        ]
                    ]
                ]
            ]
        ]

        def vocab = [
            "@graph": [
                ["@id": "http://example.org/ns/Publication",
                 "subClassOf": ["@id": "http://example.org/ns/QualifiedRole"]],
                ["@id": "http://example.org/ns/Title",
                 "subClassOf": ["@id": "http://example.org/ns/StructuredValue"]],
                ["@id": "http://example.org/ns/hasTitle",
                 "@type": "ObjectProperty",
                 "range": ["@id": "http://example.org/ns/Title"]],
                ["@id": "http://example.org/ns/publication",
                 "@type": "ObjectProperty",
                 "range": ["@id": "http://example.org/ns/Publication"]],
                ["@id": "http://example.org/ns/agent",
                 "@type": "ObjectProperty",
                 "range": ["@id": "http://example.org/ns/Agent"]],
            ]
        ]

        def jsonld = new JsonLd(context, display, vocab)
        def lensBoost = new ESQueryLensBoost(jsonld)

        when:
        def boostFields = lensBoost.computeBoostFieldsFromLenses(["Instance"] as String[])

        then:
        boostFields == ['_str^100', 'hasTitle._str^200', 'comment^200', 'publication._str^10']
    }

}
