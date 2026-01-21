package whelk.search2.querytree

import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.EsMappings
import whelk.search2.VocabMappings

import java.util.stream.Stream

class TestData {
    static def excludeFilter = new FilterAlias("excludeA", "NOT p1:A", [:])
    static def includeFilter = new FilterAlias("includeA", "NOT excludeA", [:])
    static def XYFilter = new FilterAlias("XY", "p1:X p3:Y", [:])

    static def getDisambiguate() {
        def propertyMappings = [
                'p1'              : ['p1'] as Set,
                'p1label'         : ['p1'] as Set,
                'p2'              : ['p2'] as Set,
                'p3'              : ['p3'] as Set,
                'p4'              : ['p4'] as Set,
                'p5'              : ['p5'] as Set,
                'p6'              : ['p6'] as Set,
                'p7'              : ['p7'] as Set,
                'p8'              : ['p8'] as Set,
                'p9'              : ['p9'] as Set,
                'p10'             : ['p10'] as Set,
                'p11'             : ['p11'] as Set,
                'p12'             : ['p12'] as Set,
                'p13'             : ['p13'] as Set,
                'p14'             : ['p14'] as Set,
                'type'            : ['rdf:type'] as Set,
                'rdf:type'        : ['rdf:type'] as Set,
                'instanceof'      : ['instanceOf'] as Set,
                'hasinstance'     : ['hasInstance'] as Set,
                'p'               : ['p', 'p1'] as Set,
                'plabel'          : ['p2', 'p3'] as Set,
                'pp'              : ['p3', 'p4'] as Set,
                'category'        : ['category'] as Set,
                'findcategory'    : ['librissearch:findCategory'] as Set,
                'identifycategory': ['librissearch:identifyCategory'] as Set,
                'nonecategory'    : ['librissearch:noneCategory'] as Set,
                'p3p1'            : ['p3p1'] as Set
        ]
        def classMappings = [
                't1' : ['T1'] as Set,
                't2' : ['T2'] as Set,
                't3' : ['T3'] as Set,
                't1x': ['T1x'] as Set,
                't2x': ['T2x'] as Set,
                't3x': ['T3x'] as Set,
                't'  : ['T', 'T1'] as Set,
                'tt' : ['T', 'T1'] as Set
        ]
        def enumMappings = [
                'e1': ['E1'] as Set,
                'e2': ['E2'] as Set
        ]

        def insertNamespace = m -> m.keySet().each { k -> m.put(k, ['https://id.kb.se/vocab/': m[k]]) }
        Stream.of(propertyMappings, classMappings, enumMappings).forEach(insertNamespace)

        def propertiesRestrictedByValue = [
                'category': [
                        'https://id.kb.se/term/ktg/X': ['librissearch:findCategory'],
                        'https://id.kb.se/term/ktg/Y': ['librissearch:identifyCategory']
                ]
        ]

        def vocabMappings = new VocabMappings(propertyMappings, classMappings, enumMappings, propertiesRestrictedByValue)

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
                                [
                                        '@list': [
                                            ['@id': 'p3'],
                                            ['@id': 'p4']]
                                ]
                        ]
                ],
                [
                        '@id'   : 'p7',
                        '@type' : 'DatatypeProperty',
                        'domain': [['@id': 'T1']],
                        'range' : [['@id': 'T4']]
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
                        'propertyChainAxiom': [ [ '@list': [
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
                        ]]]
                ],
                [
                        '@id'               : 'p11',
                        '@type'             : 'ObjectProperty',
                        'category'          : ['@id': "https://id.kb.se/vocab/shorthand"],
                        'propertyChainAxiom': [ [ '@list' : [
                                [
                                        'range'        : [['@id': 'T3']],
                                        'subPropertyOf': [['@id': 'p3']]
                                ],
                                ['@id': 'p4']
                        ]]]
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
                        '@id'  : 'p14',
                        '@type': 'DatatypeProperty',
                        'domain': ['@id': 'T4']
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
                [
                        '@id'     : 'hasComponent',
                        '@type'   : 'ObjectProperty',
                        'category': ['@id': 'integral'],
                        'domain'  : ['@id': 'T4'],
                        'range'   : ['@id': 'T4']
                ],
                [
                        '@id'  : 'category',
                        '@type': 'ObjectProperty'
                ],
                [
                        '@id'               : 'hasInstanceCategory',
                        'category'          : ['@id': "https://id.kb.se/vocab/shorthand"],
                        'domain'            : ['@id': 'T2'],
                        '@type'             : 'ObjectProperty',
                        'propertyChainAxiom': [['@list': [
                                ['@id': 'hasInstance'],
                                ['@id': 'category']
                        ]]]
                ],
                [
                        '@id'          : 'librissearch:findCategory',
                        '@type'        : 'ObjectProperty',
                        'subPropertyOf': [['@id': 'category']],
                        'domain'       : ['@id': 'T2'],
                        'ls:indexKey'  : '_categoryByCollection.find'
                ],
                [
                        '@id'          : 'librissearch:identifyCategory',
                        '@type'        : 'ObjectProperty',
                        'subPropertyOf': [['@id': 'category']],
                        'domain'       : ['@id': 'T2'],
                        'ls:indexKey'  : '_categoryByCollection.identify'
                ],
                [
                        '@id'          : 'librissearch:noneCategory',
                        '@type'        : 'ObjectProperty',
                        'subPropertyOf': [['@id': 'category']],
                        'domain'       : ['@id': 'T2'],
                        'ls:indexKey'  : '_categoryByCollection.@none'
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
                ['@id': 'T4', '@type': 'Class'],
                ['@id': 'E1', '@type': 'Class'],
                ['@id': 'E2', '@type': 'Class'],
                ['@id': 'p', '@type': 'DatatypeProperty'],
                [
                        '@id'               : 'p3p1',
                        '@type'             : 'DatatypeProperty',
                        'category'          : ['@id': "https://id.kb.se/vocab/shorthand"],
                        'propertyChainAxiom': [ [ '@list': [
                                ['@id': 'p3'],
                                ['@id': 'p1']
                        ]]]
                ]
        ]]
        def ctx = [
                '@context': [
                        '@vocab': 'https://id.kb.se/vocab/',
                        'p2'    : ['@type': '@vocab']
                ]
        ]
        return new JsonLd(ctx, [:], vocab)
    }

    static def getEsMappings() {
        def mappings = [
                'properties': [
                        'p3'                    : ['type': 'nested'],
                        '@reverse.instanceOf.p3': ['type': 'nested']
                ]
        ]
        return new EsMappings(mappings)
    }
}
