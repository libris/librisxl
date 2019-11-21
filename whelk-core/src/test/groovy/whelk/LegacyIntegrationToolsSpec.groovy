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
             "category": ["@id": "http://example.org/ns/pending"]]
        ]
    ]

    def tool = new LegacyIntegrationTools()

    def "should encode and crc32 hash identifier"() {
        expect:
        tool.generateId(data) == id
        where:
        data                | id
        "/auth/1"           | "hftwp11146kkhc1"
        "/auth/12345"       | "zw9c5x6h3bj0c87"
        "/bib/12345"        | "5ng15x6h05ft7lr"
        "/hold/12345"       | "bgmp5x6h1ct51qd"
        "/auth/123551211"   | "53nnvnsp2gq1qpz"
        "/hold/999999999"   | "59snjbd943p7s0x"
    }

    def "should get marc category for term"() {
        expect:
        tool.getMarcCollectionForTerm([category: cats]) == id
        where:
        id      | cats
        'bib'   | ['@id': "$MARC/bib"]
        'bib'   | [['@id': "$MARC/bib"], ['@id': "pending"]]
        'auth'  | [['@id': "$MARC/auth"]]
        null    | ['@id': 'other']
        null    | [['@id': 'other']]
        null    | []
        null    | null
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
    }

}
