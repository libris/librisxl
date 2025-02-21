package whelk.importer

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
        base.data == ['@graph': [
                ['modified': '2022-02-02T12:00:00Z'],
                ['a': "something third"]
        ]]
    }

    def "don't replace with lower priority"() {
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
                                ["operation": "replace", "path": ["@graph",1,"a"], "priority": ["sigel1": 4, "sigel2": 5, "sigel3": 3]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel3", history)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-02T12:00:00Z'],
                ['a': "something second"]
        ]]
    }

    def "don't replace a hand edit"() {
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
                 'changedIn': 'xl',
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
        base.data == ['@graph': [
                ['modified': '2022-02-02T12:00:00Z'],
                ['a': "something second"]
        ]]
    }

    def "do replace a hand edit, if we're ignoring existing history (passing as null)"() {
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
                 'changedIn': 'xl',
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-02T12:00:00Z'],
                                 ['a': "something second"]
                         ]]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
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
        merge.merge(base, incoming, "sigel3", null)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-02T12:00:00Z'],
                ['a': "something third"]
        ]]
    }

    def "simple add"() {
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
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)

        def incoming = new Document( (Map)
                ['@graph': [
                        ['modified': '2022-03-01T12:00:00Z'],
                        ['b': "something else"]
                ]]
        )
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"b"]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-01T12:00:00Z'],
                ['a': "something", 'b': "something else"]
        ]]
    }

    def "add with higher priority"() {
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
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        ['modified': '2022-03-01T12:00:00Z'],
                        ['b': "something else"]
                ]]
        )
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1], "priority": ["sigel1": 1, "sigel2": 2]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-01T12:00:00Z'],
                ['a': "something", 'b': "something else"]
        ]]
    }

    def "add with lower priority"() {
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
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        ['modified': '2022-03-01T12:00:00Z'],
                        ['b': "something else"]
                ]]
        )
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1], "priority": ["sigel1": 2, "sigel2": 1]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-01T12:00:00Z'],
                ['a': "something"]
        ]]
    }

    def "add with lower priority, but ignoring history"() {
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
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def incoming = new Document( (Map)
                ['@graph': [
                        ['modified': '2022-03-01T12:00:00Z'],
                        ['b': "something else"]
                ]]
        )
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1], "priority": ["sigel1": 2, "sigel2": 1]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", null)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-01T12:00:00Z'],
                ['a': "something", 'b': "something else"]
        ]]
    }

    def "replace using @typed path"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-01T12:00:00Z'],
                                 [ "prop": [
                                         ['@type': "SomeType", 'a': "something"],
                                         ['@type': "OtherType", 'a': "something else"]
                                 ]]
                         ]]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        ['modified': '2022-03-01T12:00:00Z'],
                        [ "prop": [
                                // Reversed order of list
                                ['@type': "OtherType", 'a': "something else"],
                                ['@type': "SomeType", 'a': "this should change"]
                        ]]
                ]]
        )
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "replace", "path": ["@graph",1,"prop","@type=SomeType","a"], "priority": ["sigel1": 1, "sigel2": 2]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-01T12:00:00Z'],
                [ "prop": [
                        ['@type': "SomeType", 'a': "this should change"],
                        ['@type': "OtherType", 'a': "something else"]
                ]]
        ]]
    }

    def "update with same priority"() {
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
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)

        def incoming = new Document( (Map)
                ['@graph': [
                        ['modified': '2022-03-01T12:00:00Z'],
                        ['a': "something else"]
                ]]
        )
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "replace", "path": ["@graph",1,"a"], "priority": ["sigel1":1, "sigel2":1]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-01T12:00:00Z'],
                ['a': "something else"]
        ]]
    }

    def "don't re-add something manually removed from list"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-01T12:00:00Z'],
                                 ['a': ["something"]]
                         ]]
                ],
                ['changedBy': 'sigel1',
                 'changedIn': 'xl', // a hand edit
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-02T12:00:00Z'],
                                 ['a': []] // sigel1 removed something and claimed the list in a hand edit
                         ]]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        ['modified': '2022-03-03T12:00:00Z'],
                        ['a': ["something else"]]
                ]]
        )
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"a"]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-02T12:00:00Z'],
                ['a': []]
        ]]
    }

    def "don't re-add a manually removed property"() {
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
                ['changedBy': 'sigel1',
                 'changedIn': 'xl', // a hand edit
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-01T12:00:00Z'],
                                 [] // sigel1 removed something and claimed the object in a hand edit
                         ]]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        ['modified': '2022-03-01T12:00:00Z'],
                        ['a': "something else"]
                ]]
        )
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"a"], "priority": ["sigel1": 1, "sigel2": 1]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-01T12:00:00Z'],
                []
        ]]
    }

    def "rules stack"() {
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
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)

        def incoming = new Document( (Map)
                ['@graph': [
                        ['modified': '2022-03-01T12:00:00Z'],
                        ['b': "something else"]
                ]]
        )
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,]] // Does not point all the way to b, a parent is enough
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == ['@graph': [
                ['modified': '2022-02-01T12:00:00Z'],
                ['a': "something", 'b': "something else"]
        ]]
    }

    def "add subtitle with higher priority"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitle': 'Huvudtitel']
                                         ],
                                         'instanceOf': [
                                                 'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                                 'contribution': [
                                                         ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                                 ]
                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                [
                        'modified': '2022-02-01T12:00:00Z',
                        'mainEntity': 'meID'
                ],
                [
                        '@id': 'meID',
                        'hasTitle': [
                                ['@type': 'Title', 'mainTitle': 'Huvudtitel', 'subtitle': 'Undertitel']
                        ],
                        'instanceOf': [
                                'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                'contribution': [
                                        ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                ]
                        ]
                ]
        ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"hasTitle"], "priority": ["sigel1": 1, "sigel2": 2]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                [
                        'modified': '2022-02-01T12:00:00Z',
                        'mainEntity': 'meID'
                ],
                [
                        '@id': 'meID',
                        'hasTitle': [
                                ['@type': 'Title', 'mainTitle': 'Huvudtitel', 'subtitle': 'Undertitel']
                        ],
                        'instanceOf': [
                                'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                'contribution': [
                                        ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                ]
                        ]
                ]
        ]]
    }

    def "add subtitle with equal priority"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitle': 'Huvudtitel']
                                         ],
                                         'instanceOf': [
                                                 'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                                 'contribution': [
                                                         ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                                 ]
                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'Huvudtitel', 'subtitle': 'Undertitel']
                                ],
                                'instanceOf': [
                                        'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                        'contribution': [
                                                ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                        ]
                                ]
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"hasTitle"], "priority": ["sigel1": 1, "sigel2": 1]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'Huvudtitel', 'subtitle': 'Undertitel']
                                ],
                                'instanceOf': [
                                        'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                        'contribution': [
                                                ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                        ]
                                ]
                        ]
                ]]
    }

    def "don't add subtitle with lower priority"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitle': 'Huvudtitel']
                                         ],
                                         'instanceOf': [
                                                 'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                                 'contribution': [
                                                         ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                                 ]
                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'Huvudtitel', 'subtitle': 'Undertitel']
                                ],
                                'instanceOf': [
                                        'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                        'contribution': [
                                                ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                        ]
                                ]
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"hasTitle"], "priority": ["sigel1": 2, "sigel2": 1]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'Huvudtitel']
                                ],
                                'instanceOf': [
                                        'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                        'contribution': [
                                                ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                        ]
                                ]
                        ]
                ]]
    }

    def "add primary contribution"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitle': 'Huvudtitel']
                                         ],
                                         'instanceOf': [
                                                 'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                                 'contribution': [
                                                         ['@type': 'Contribution', 'agent': ['@id': 'some id']]
                                                 ]
                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'Huvudtitel']
                                ],
                                'instanceOf': [
                                        'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                        'contribution': [
                                                ['@type': 'PrimaryContribution', 'agent': ['@id': 'other id']],
                                                ['@type': 'Contribution', 'agent': ['@id': 'some id']]
                                        ]
                                ]
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"instanceOf", "contribution", "@type=PrimaryContribution"], "priority": ["sigel1": 1, "sigel2": 2]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'Huvudtitel']
                                ],
                                'instanceOf': [
                                        'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                        'contribution': [
                                                ['@type': 'Contribution', 'agent': ['@id': 'some id']],
                                                ['@type': 'PrimaryContribution', 'agent': ['@id': 'other id']]
                                        ]
                                ]
                        ]
                ]]
    }

    def "don't add manually removed subtitle"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitle': 'Huvudtitel', 'subtitle': 'Undertitel']
                                         ],
                                         'instanceOf': [
                                                 'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                                 'contribution': [
                                                         ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                                 ]
                                         ]
                                 ]
                         ]
                 ]
                ],
                ['changedBy': 'sigel1',
                 'changedIn': 'xl', // A manual edit!
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-02T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitle': 'Huvudtitel'] // Subtitle removed!
                                         ],
                                         'instanceOf': [
                                                 'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                                 'contribution': [
                                                         ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                                 ]
                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-03T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'Huvudtitel', 'subtitle': 'Undertitel']
                                ],
                                'instanceOf': [
                                        'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                        'contribution': [
                                                ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                        ]
                                ]
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"hasTitle","@type=Title","subtitle"], "priority": ["sigel1": 2, "sigel2": 3]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-02T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'Huvudtitel'] // Manually removed subtitle not re added!
                                ],
                                'instanceOf': [
                                        'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                        'contribution': [
                                                ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                        ]
                                ]
                        ]
                ]]
    }

    def "don't add manually removed subtitle2"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitle': 'Huvudtitel', 'subtitle': 'Undertitel']
                                         ],
                                         'instanceOf': [
                                                 'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                                 'contribution': [
                                                         ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                                 ]
                                         ]
                                 ]
                         ]
                 ]
                ],
                ['changedBy': 'sigel1',
                 'changedIn': 'xl', // A manual edit!
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-02T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitle': 'Huvudtitel'] // Subtitle removed!
                                         ],
                                         'instanceOf': [
                                                 'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                                 'contribution': [
                                                         ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                                 ]
                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-03T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'Huvudtitel', 'subtitle': 'Undertitel']
                                ],
                                'instanceOf': [
                                        'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                        'contribution': [
                                                ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                        ]
                                ]
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"hasTitle"], "priority": ["sigel1": 2, "sigel2": 3]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-02T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'Huvudtitel'] // Manually removed subtitle not re added!
                                ],
                                'instanceOf': [
                                        'subject': ['@id': 'https://id.kb.se/term/sao/Fayyumportr%C3%A4tt'],
                                        'contribution': [
                                                ['@type': 'PrimaryContribution', 'agent': ['@id': 'https://libris-qa.kb.se/rp355vx91wjt2ms#it']]
                                        ]
                                ]
                        ]
                ]]
    }

    def "don't add manually removed translationOf"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'instanceOf': [
                                                 'translationOf' : ['@id': 'other ID'],
                                         ]
                                 ]
                         ]
                 ]
                ],
                ['changedBy': 'sigel1',
                 'changedIn': 'xl', // A manual edit!
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-02T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'instanceOf': [

                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-03T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'instanceOf': [
                                        'translationOf' : ['@id': 'other ID'],
                                ]
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"instanceOf","translationOf"]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-02T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'instanceOf': [
                                ]
                        ]
                ]]
    }

    def "don't add manually removed extent"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'extent': 'some extent'
                                 ]
                         ]
                 ]
                ],
                ['changedBy': 'sigel1',
                 'changedIn': 'xl', // A manual edit!
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-02T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-03T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'extent': 'some extent'
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"extent"]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-02T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                        ]
                ]]
    }

    def "don't add mainTitle on top of mainTitleByLang"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitleByLang':
                                                         [
                                                                 "el": "Η κόρη της Ανθής Αλκαίου",
                                                                 "el-Latn-t-el": "I kori tis Anthis Alkeou"
                                                         ]
                                                 ]
                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'I kori tis Anthis Alkeou']
                                ]
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"hasTitle","@type=Title"]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitleByLang':
                                                [
                                                        "el": "Η κόρη της Ανθής Αλκαίου",
                                                        "el-Latn-t-el": "I kori tis Anthis Alkeou"
                                                ]
                                        ]
                                ]
                        ]
                ]]
    }

    def "don't add mainTitleByLang on top of mainTitle"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitle': 'I kori tis Anthis Alkeou']
                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle':
                                                [
                                                        "el": "Η κόρη της Ανθής Αλκαίου",
                                                        "el-Latn-t-el": "I kori tis Anthis Alkeou"
                                                ]
                                        ]
                                ]
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "add_if_none", "path": ["@graph",1,"hasTitle","@type=Title"]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'I kori tis Anthis Alkeou']
                                ]
                        ]
                ]]
    }

    def "replace mainTitleByLang with mainTitle"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitleByLang':
                                                         [
                                                                 "el": "Η κόρη της Ανθής Αλκαίου",
                                                                 "el-Latn-t-el": "I kori tis Anthis Alkeou"
                                                         ]
                                                 ]
                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'I kori tis Anthis Alkeou']
                                ]
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "replace", "path": ["@graph",1,"hasTitle","@type=Title"], "priority": ["sigel1": 1, "sigel2": 2]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitle': 'I kori tis Anthis Alkeou']
                                ]
                        ]
                ]]
    }

    def "replace mainTitle with mainTitleByLang"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'batch import',
                 'data': [
                         '@graph': [
                                 [
                                         'modified': '2022-02-01T12:00:00Z',
                                         'mainEntity': 'meID'
                                 ],
                                 [
                                         '@id': 'meID',
                                         'hasTitle': [
                                                 ['@type': 'Title', 'mainTitle': 'I kori tis Anthis Alkeou']
                                         ]
                                 ]
                         ]
                 ]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }
        def history = new History(versions, ld)
        def incoming = new Document( (Map)
                ['@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitleByLang':
                                                [
                                                        "el": "Η κόρη της Ανθής Αλκαίου",
                                                        "el-Latn-t-el": "I kori tis Anthis Alkeou"
                                                ]
                                        ]
                                ]
                        ]
                ]])
        Document base = versions.last().doc
        Merge merge = new Merge(
                [
                        "rules": [
                                ["operation": "replace", "path": ["@graph",1,"hasTitle","@type=Title"], "priority": ["sigel1": 1, "sigel2": 2]]
                        ]
                ]
        )
        merge.merge(base, incoming, "sigel2", history)
        expect:
        base.data == [
                '@graph': [
                        [
                                'modified': '2022-02-01T12:00:00Z',
                                'mainEntity': 'meID'
                        ],
                        [
                                '@id': 'meID',
                                'hasTitle': [
                                        ['@type': 'Title', 'mainTitleByLang':
                                                [
                                                        "el": "Η κόρη της Ανθής Αλκαίου",
                                                        "el-Latn-t-el": "I kori tis Anthis Alkeou"
                                                ]
                                        ]
                                ]
                        ]
                ]]
    }
}
