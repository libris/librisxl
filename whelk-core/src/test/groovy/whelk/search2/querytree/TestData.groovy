package whelk.search2.querytree

import whelk.JsonLd
import whelk.search2.AppParams
import whelk.search2.Disambiguate
import whelk.search2.EsMappings
import whelk.search2.VocabMappings

class TestData {
    static def excludeFilter = new FilterAlias("excludeA", "NOT p1:A", [:])
    static def includeFilter = new FilterAlias("includeA", "NOT excludeA", [:])
    static def XYFilter = new FilterAlias("XY", "p1:X p3:Y", [:])

    static def getDisambiguate() {
        def propertyAliasMappings = [
                'p1'         : 'p1',
                'p1label'    : 'p1',
                'p2'         : 'p2',
                'p3'         : 'p3',
                'p4'         : 'p4',
                'p5'         : 'p5',
                'p6'         : 'p6',
                'p7'         : 'p7',
                'p8'         : 'p8',
                'p9'         : 'p9',
                'p10'        : 'p10',
                'p11'        : 'p11',
                'p12'        : 'p12',
                'p13'        : 'p13',
                'type'       : 'rdf:type',
                'rdf:type'   : 'rdf:type',
                'instanceof' : 'instanceOf',
                'hasinstance': 'hasInstance'
        ]
        def ambiguousPropertyAliases = [
                'p'     : ['p', 'p1'] as Set,
                'plabel': ['p2', 'p3'] as Set,
                'pp'    : ['p3', 'p4'] as Set
        ]
        def classAliasMappings = [
                't1' : 'T1',
                't2' : 'T2',
                't3' : 'T3',
                't1x': 'T1x',
                't2x': 'T2x',
                't3x': 'T3x'
        ]
        def ambiguousClassAliases = [
                't' : ['T', 'T1'] as Set,
                'tt': ['T', 'T1'] as Set
        ]
        def enumAliasMappings = [
                'e1': 'E1',
                'e2': 'E2'
        ]
        def vocabMappings = new VocabMappings([
                "propertyAliasMappings"   : propertyAliasMappings,
                "classAliasMappings"      : classAliasMappings,
                "enumAliasMappings"       : enumAliasMappings,
                "ambiguousPropertyAliases": ambiguousPropertyAliases,
                "ambiguousClassAliases"   : ambiguousClassAliases
        ])

        def filterAliases = [
                excludeFilter,
                includeFilter,
                XYFilter
        ]

        return new Disambiguate(vocabMappings, filterAliases, getJsonLd())
    }

    static def getJsonLd() {
        def vocab = ['@graph': [
                ['@id': 'p1', '@type': 'DatatypeProperty'],
                ['@id': 'p2', '@type': 'ObjectProperty', 'librisQueryCode': 'P2'],
                ['@id': 'p3', '@type': 'ObjectProperty'],
                ['@id': 'p4', '@type': 'ObjectProperty'],
                ['@id': 'p5', '@type': 'ObjectProperty', 'domain': ['@id': 'https://id.kb.se/vocab/AdminMetadata']],
                [
                        '@id'               : 'p6',
                        '@type'             : 'ObjectProperty',
                        'category'          : ['@id': "https://id.kb.se/vocab/shorthand"],
                        'propertyChainAxiom': [
                                ['@id': 'p3'],
                                ['@id': 'p4']
                        ]
                ],
                [
                        '@id'   : 'p7',
                        '@type' : 'DatatypeProperty',
                        'domain': [['@id': 'T1']]
                ],
                [
                        '@id'   : 'p8',
                        '@type' : 'DatatypeProperty',
                        'domain': [['@id': 'T2']]
                ],
                [
                        '@id'   : 'p9',
                        '@type' : 'DatatypeProperty',
                        'domain': [['@id': 'T3']]
                ],
                [
                        '@id'               : 'p10',
                        '@type'             : 'DatatypeProperty',
                        'category'          : ['@id': "https://id.kb.se/vocab/shorthand"],
                        'propertyChainAxiom': [
                                [
                                        'range'        : [
                                                [
                                                        'subClassOf': [
                                                                [
                                                                        '@type'     : 'Restriction',
                                                                        'hasValue'  : [
                                                                                '@id': 'https://id.kb.se/x'
                                                                        ],
                                                                        "onProperty": [
                                                                                '@id': 'p3'
                                                                        ]
                                                                ]
                                                        ]
                                                ]
                                        ],
                                        'subPropertyOf': [
                                                [
                                                        '@id': 'p4'
                                                ]
                                        ]
                                ],
                                ['@id': 'p1']
                        ]
                ],
                [
                        '@id'               : 'p11',
                        '@type'             : 'ObjectProperty',
                        'category'          : ['@id': "https://id.kb.se/vocab/shorthand"],
                        'propertyChainAxiom': [
                                [
                                        'range'        : [['@id': 'T3']],
                                        'subPropertyOf': [['@id': 'p3']]
                                ],
                                ['@id': 'p4']
                        ]
                ],
                [
                        '@id'  : 'p12',
                        '@type': 'DatatypeProperty',
                        'range': ['@id': 'xsd:dateTime']
                ],
                [
                        '@id'  : 'p13',
                        '@type': 'ObjectProperty',
                        'range': ['@id': 'T1']
                ],
                [
                        '@id'      : 'instanceOf',
                        '@type'    : 'ObjectProperty',
                        'category' : ['@id': 'integral'],
                        'domain'   : ['@id': 'T1'],
                        'range'    : ['@id': 'T2'],
                        'inverseOf': ['@id': 'hasInstance']
                ],
                [
                        '@id'      : 'hasInstance',
                        '@type'    : 'ObjectProperty',
                        'category' : ['@id': 'integral'],
                        'domain'   : ['@id': 'T2'],
                        'range'    : ['@id': 'T1'],
                        'inverseOf': ['@id': 'instanceOf']
                ],
                ['@id': 'textQuery', '@type': 'DatatypeProperty'],
                ['@id': 'rdf:type', '@type': 'ObjectProperty'],
                ['@id': 'meta', '@type': 'ObjectProperty'],
                ['@id': 'T1', '@type': 'Class'],
                ['@id': 'T2', '@type': 'Class'],
                ['@id': 'T3', '@type': 'Class'],
                ['@id': 'T1x', '@type': 'Class', 'subClassOf': [['@id': 'T1']]],
                ['@id': 'T2x', '@type': 'Class', 'subClassOf': [['@id': 'T2']]],
                ['@id': 'T3x', '@type': 'Class', 'subClassOf': [['@id': 'T3']]],
                ['@id': 'E1', '@type': 'Class'],
                ['@id': 'E2', '@type': 'Class'],
                ['@id': 'p', '@type': 'DatatypeProperty']
        ]]
        def ctx = [
                '@context': [
                        '@vocab': 'https://id.kb.se/vocab/',
                        'p2': ['@type': '@vocab']
                ]
        ]
        return new JsonLd(ctx, [:], vocab)
    }

    static def getEsMappings() {
        def mappings = [
                'properties': [
                        'p3': ['type': 'nested']
                ]
        ]
        return new EsMappings(mappings)
    }

    static def getAppParams() {
        def appConfig = [
                'statistics': [
                        'sliceList': [
                            [ 'dimensionChain' : ['rdf:type']],
                            [ 'dimensionChain' : ['p2']],
                            [ 'dimensionChain' : ['p6']],
                        ]
                ]
        ]

        return new AppParams(appConfig, getJsonLd())
    }
}
