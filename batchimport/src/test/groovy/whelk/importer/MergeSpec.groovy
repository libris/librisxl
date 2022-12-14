package whelk.importer;

import spock.lang.Specification
import whelk.Document
import whelk.JsonLd
import whelk.history.DocumentVersion
import whelk.history.History

class MergeSpec extends Specification {

    static final Map CONTEXT_DATA = [
            "@context": [
                    "@vocab": "http://example.org/ns/",
                    "pfx": "http://example.org/pfx/"
            ]
    ]

    static final Map VOCAB_DATA = [
            "@graph": [
                    ["@id": "http://example.org/ns/ProvisionActivity",
                     "subClassOf": [ ["@id": "http://example.org/ns/Event"] ]],
                    ["@id": "http://example.org/ns/Publication",
                     "subClassOf": ["@id": "http://example.org/ns/ProvisionActivity"]],
                    ["@id": "http://example.org/pfx/SpecialPublication",
                     "subClassOf": ["@id": "http://example.org/ns/Publication"]],

                    ["@id": "http://example.org/ns/label", "@type": ["@id": "DatatypeProperty" ]],
                    ["@id": "http://example.org/ns/prefLabel",
                     "subPropertyOf": [ ["@id": "http://example.org/ns/label"] ]],
                    ["@id": "http://example.org/ns/preferredLabel",
                     "subPropertyOf": ["@id": "http://example.org/ns/prefLabel"]],
                    ["@id": "http://example.org/ns/name",
                     "subPropertyOf": ["@id": "http://example.org/ns/label"]]
            ]
    ]

    def "simple replace property"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-01T12:00:00Z'],
                                 ['a': "something"]
                         ]]
                ],
                ['changedBy': 'sigel2',
                 'changedIn': 'batch import',
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-02T12:00:00Z'],
                                 ['a': "something second"]
                         ]]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }

        def history = new History(versions, ld)

        def incoming = new Document( (Map)
                ['@graph': [
                        ['modified': '2022-03-01T12:00:00Z'],
                        ['a': "something third"]
                ]]
        )

        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "replace", "path": ["@graph",1,"a"], "priority": ["sigel1": 1, "sigel2": 2, "sigel3": 3]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel3", history)

        expect:
        base.data["@graph"][1]["a"] == "something third"
    }
}
