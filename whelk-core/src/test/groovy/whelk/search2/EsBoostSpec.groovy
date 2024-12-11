package whelk.search2

import spock.lang.Specification
import whelk.JsonLd

class EsBoostSpec extends Specification {
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
                                        "Instance"   : [
                                                "classLensDomain": "Instance",
                                                "showProperties" : ["hasTitle", "comment"]
                                        ],
                                        "Publication": [
                                                "classLensDomain": "Publication",
                                                "showProperties" : ["agent"]
                                        ]
                                ]
                        ],
                        "cards": [
                                "lenses": [
                                        "Instance": [
                                                "classLensDomain": "Instance",
                                                "showProperties" : [
                                                        [
                                                                "alternateProperties": [
                                                                        [
                                                                                "subPropertyOf": "hasTitle"
                                                                        ],
                                                                        [
                                                                                "subPropertyOf": "value"
                                                                        ],
                                                                        [
                                                                                "noise": "should be ignored"
                                                                        ],
                                                                        "hasTitle",
                                                                        "value"
                                                                ]
                                                        ],
                                                        "publication"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ]

        def vocab = [
                "@graph": [
                        ["@id": ns + "QualifiedRole", "@type": "Class"],
                        ["@id"       : ns + "Publication", "@type": "Class",
                         "subClassOf": ["@id": ns + "QualifiedRole"]],
                        ["@id"       : ns + "Title", "@type": "Class",
                         "subClassOf": ["@id": ns + "StructuredValue"]],
                        ["@id"  : ns + "hasTitle", "@type": "ObjectProperty",
                         "range": [["@id": ns + "Title"]]],
                        ["@id"  : ns + "publication", "@type": "ObjectProperty",
                         "range": [["@id": ns + "Publication"]]],
                        ["@id"  : ns + "agent", "@type": "ObjectProperty",
                         "range": [["@id": ns + "Agent"]]],
                        ["@id": ns + "value", "@type": "DatatypeProperty"],
                        ["@id": ns + "comment", "@type": "DatatypeProperty"]
                ]
        ]

        def jsonld = new JsonLd(context, display, vocab)
        def lensBoost = new EsBoost(jsonld)

        when:
        def boostFields = lensBoost.getBoostFields(["Instance"])

        then:
        boostFields == [
                'comment^200', 'hasTitle._str^200', '_str^100', 'publication.agent._str^10', 'value^10'
        ]
    }
}
