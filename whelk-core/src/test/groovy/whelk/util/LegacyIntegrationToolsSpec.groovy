package whelk.util

import spock.lang.Specification

import whelk.JsonLd

class LegacyIntegrationToolsSpec extends Specification {

    static final Map CONTEXT_DATA = [
        "@context": ["@vocab": "http://example.org/ns/"]
    ]

    static final String MARC = 'https://id.kb.se/marc'

    static final Map VOCAB_DATA = [
        "@graph": [
            ["@id": "http://example.org/ns/Instance",
             "category": [ ["@id": "$MARC/bib"] ]],
            ["@id": "http://example.org/ns/Print",
             "subClassOf": [ ["@id": "http://example.org/ns/Instance"] ],
             "category": [ ["@id": "http://example.org/ns/"] ]],
            ["@id": "http://example.org/ns/Paperback",
             "subClassOf": [ ["@id": "http://example.org/ns/Print"] ],
             "category": ["@id": "http://example.org/ns/pending"]],
            ["@id": "http://example.org/ns/None",
             "subClassOf": [ ["@id": "http://example.org/ns/Instance"] ],
             "category": [ ["@id": "$MARC/none"] ]],
        ]
    ]

    def tool = new LegacyIntegrationTools()

    def "should get marc category for term"() {
        expect:
        tool.getMarcCollectionForTerm([category: cats]) == id
        where:
        id          | cats
        'bib'       | ['@id': "$MARC/bib"]
        'bib'       | [['@id': "$MARC/bib"], ['@id': "pending"]]
        'auth'      | [['@id': "$MARC/auth"]]
        'undefined' | ['@id': 'other']
        'undefined' | [['@id': 'other']]
        'undefined' | []
        'undefined' | null
    }

    def "should get marc collection for type"() {
        expect:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        tool.getMarcCollectionInHierarchy(type, ld) == collection
        where:
        type        | collection
        'Instance'  | 'bib'
        'Print'     | 'bib'
        'Paperback' | 'bib'
        'Other'     | 'none'
        'None'      | 'none'
    }

}
