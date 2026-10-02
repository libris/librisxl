package whelk.search2

import whelk.JsonLd
import whelk.search2.esquery.ESMappings
import whelk.search2.querytree.node.FilterAlias

import java.util.stream.Stream

class TestData {
    static def excludeFilter = new FilterAlias("excludeA", "NOT p1:A", [:])
    static def includeFilter = new FilterAlias("includeA", "NOT excludeA", [:])
    static def XYFilter = new FilterAlias("XY", "p1:X p3:Y", [:])

    static def getDisambiguate() {
        def jsonLd = getJsonLd()

        def propertyMappings = [
                'p1'              : ['p1'],
                'p1label'         : ['p1'],
                'p2'              : ['p2'],
                'p3'              : ['p3'],
                'p4'              : ['p4'],
                'p5'              : ['p5'],
                'p6'              : ['p6'],
                'p7'              : ['p7'],
                'p8'              : ['p8'],
                'p9'              : ['p9'],
                'p10'             : ['p10'],
                'p11'             : ['p11'],
                'p12'             : ['p12'],
                'p13'             : ['p13'],
                'p14'             : ['p14'],
                'p15'             : ['p15'],
                'p16'             : ['p16'],
                'ctx.p'           : ['ctxProp'],
                'type'            : ['rdf:type'],
                'rdf:type'        : ['rdf:type'],
                'instanceof'      : ['instanceOf'],
                'hasinstance'     : ['hasInstance'],
                'hasitem'         : ['hasItem'],
                'p'               : ['p', 'p1'],
                'plabel'          : ['p2', 'p3'],
                'pp'              : ['p3', 'p4'],
                'workcategory'    : ['librissearch:workCategory'],
                'instancecategory': ['librissearch:instanceCategory'],
                'findcategory'    : ['librissearch:findCategory'],
                'identifycategory': ['librissearch:identifyCategory'],
                'nonecategory'    : ['librissearch:noneCategory'],
                'p3p1'            : ['p3p1'],
                't1p3p1'          : ['t1p3p1'],
                'restrictedp_p1'  : ['restrictedP_p1'],
                'restrictedp_p1_2': ['restrictedP_p1_2'],
                't1type'          : ['t1Type'],
                'date'            : ['date'],
                'year'            : ['year']
        ]
        def classMappings = [
                't1' : ['T1'],
                't2' : ['T2'],
                't3' : ['T3'],
                't1x': ['T1x'],
                't2x': ['T2x'],
                't3x': ['T3x'],
                't'  : ['T', 'T1'],
                'tt' : ['T', 'T1']
        ]
        def enumMappings = [
                'e1': ['E1'],
                'e2': ['E2']
        ]

        // Group each mapped term under its own namespace, same as ResourceLookup.VocabMappings
        // does for the real vocab, e.g. "rdf:type" under "rdf", "librissearch:workCategory"
        // under "librissearch", and terms without a prefix under the default vocab namespace.
        def nsOf = term -> ResourceLookup.VocabMappings.getNs((String) term, jsonLd.getVocabPrefix())
        def insertNamespace = m -> m.keySet().each { k ->
            def terms = ((List) m[(String) k])
            def groupedByNs = terms.groupBy(nsOf).collectEntries { ns, group -> [ns, group as Set] }
            m.put(k, groupedByNs)
        }
        Stream.of(propertyMappings, classMappings, enumMappings).each(insertNamespace)

        def propertiesRestrictedByValue = [
                'librissearch:workCategory': [
                        'https://id.kb.se/term/ktg/X': ['librissearch:findCategory'],
                        'https://id.kb.se/term/ktg/Y': ['librissearch:identifyCategory']
                ]
        ]

        def vocabMappings = new ResourceLookup.VocabMappings(propertyMappings, classMappings, enumMappings, propertiesRestrictedByValue)
        def externalMappings = new ResourceLookup.ExternalMappings(['T5': ['xyz': ['@id': 'https://libris.kb.se/XYZ']]])
        def resourceLookup = new ResourceLookup(vocabMappings, externalMappings)

        def filterAliases = [
                excludeFilter,
                includeFilter,
                XYFilter
        ]

        return new Disambiguate(resourceLookup, filterAliases, jsonLd)
    }

    static def getJsonLd() {
        def vocab = ['@graph': [
                ['@id': 'p1', '@type': 'DatatypeProperty'],
                ['@id': 'p2', '@type': 'ObjectProperty', 'librisQueryCode': 'P2'],
                ['@id': 'p3', '@type': 'ObjectProperty'],
                ['@id': 'p4', '@type': 'ObjectProperty'],
                [
                        '@id'     : 'p5', '@type': 'ObjectProperty',
                        'category': ['@id': "https://id.kb.se/ns/librissearch/preferLike"]
                ],
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
                        '@id'  : 'p15',
                        '@type': 'ObjectProperty',
                        'range': ['@id': 'T5']
                ],
                [
                        '@id'        : 'p16',
                        '@type'      : 'DatatypeProperty',
                        'category'   : ['@id': "https://id.kb.se/ns/librissearch/composite"]
                ],
                [
                        '@id'        : 'p17',
                        '@type'      : 'DatatypeProperty',
                        'subPropertyOf': [['@id': 'p16']]
                ],
                [
                        '@id'        : 'p18',
                        '@type'      : 'DatatypeProperty',
                        'subPropertyOf': [['@id': 'p16']]
                ],
                [
                        '@id'  : 'p19',
                        '@type': 'ObjectProperty',
                        'domain': ['@id': 'T1']
                ],
                [
                        '@id'  : 'p20',
                        '@type': 'ObjectProperty',
                        'domain': ['@id': 'T3']
                ],
                [
                        '@id'  : 'ctxProp',
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
                        '@id'   : 'hasItem',
                        '@type' : 'DatatypeProperty',
                        'domain': [['@id': 'T1']],
                        'range' : [['@id': 'T4']]
                ],
                [
                        '@id'        : 'librissearch:workCategory',
                        'category'   : ['@id': "https://id.kb.se/ns/librissearch/composite"],
                        '@type'      : 'ObjectProperty',
                        'domain'     : ['@id': 'T2']
                ],
                [
                        '@id'        : 'librissearch:instanceCategory',
                        '@type'      : 'ObjectProperty',
                        'domain'     : ['@id': 'T1'],
                        'ls:indexKey': '_categoryByCollection.@none'
                ],
                [
                        '@id'          : 'librissearch:findCategory',
                        '@type'        : 'ObjectProperty',
                        'subPropertyOf': [['@id': 'librissearch:workCategory']],
                        'category'     : ['@id': "https://id.kb.se/ns/librissearch/coercing"],
                        'ls:indexKey'  : '_categoryByCollection.find'
                ],
                [
                        '@id'          : 'librissearch:identifyCategory',
                        '@type'        : 'ObjectProperty',
                        'subPropertyOf': [['@id': 'librissearch:workCategory']],
                        'category'     : ['@id': "https://id.kb.se/ns/librissearch/coercing"],
                        'ls:indexKey'  : '_categoryByCollection.identify'
                ],
                [
                        '@id'          : 'librissearch:noneCategory',
                        '@type'        : 'ObjectProperty',
                        'subPropertyOf': [['@id': 'librissearch:workCategory']],
                        'category'     : ['@id': "https://id.kb.se/ns/librissearch/coercing"],
                        'ls:indexKey'  : '_categoryByCollection.@none'
                ],
                ['@id': 'textQuery', '@type': 'DatatypeProperty'],
                ['@id': 'rdf:type', '@type': 'ObjectProperty'],
                ['@id': 'T1', '@type': 'Class'],
                ['@id': 'T2', '@type': 'Class'],
                ['@id': 'T3', '@type': 'Class'],
                ['@id': 'T1x', '@type': 'Class', 'subClassOf': [['@id': 'T1']]],
                ['@id': 'T2x', '@type': 'Class', 'subClassOf': [['@id': 'T2']]],
                ['@id': 'T3x', '@type': 'Class', 'subClassOf': [['@id': 'T3']]],
                ['@id': 'T4', '@type': 'Class'],
                ['@id': 'T5', '@type': 'Class'],
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
                ],
                [
                        '@id'               : 't1p3p1',
                        '@type'             : 'DatatypeProperty',
                        'category'          : ['@id': "https://id.kb.se/vocab/shorthand"],
                        'domain'            : ['@id': 'T1'],
                        'propertyChainAxiom': [['@list': [
                                [
                                        'domain'       : [['@id': 'T1']],
                                        'subPropertyOf': [['@id': 'p3']]
                                ],
                                ['@id': 'p1']
                        ]]]
                ],
                [
                        '@id'               : 'restrictedP',
                        'subPropertyOf'     : [['@id': 'p3']],
                        "range"             : [
                                [
                                        "subClassOf": [
                                                [
                                                        "@type"     : "Restriction",
                                                        "onProperty": ["@id": "p4"],
                                                        "hasValue"  : ["@id": "https://id.kb.se/x"],
                                                ]
                                        ]
                                ]
                        ]
                ],
                [
                        '@id'               : 'restrictedP_p1',
                        '@type'             : 'DatatypeProperty',
                        'category'          : ['@id': "https://id.kb.se/vocab/shorthand"],
                        'propertyChainAxiom': [['@list': [
                                ['@id': 'restrictedP'],
                                ['@id': 'p1']
                        ]]]
                ],
                [
                        '@id'               : '_:restrictedP',
                        'subPropertyOf'     : [['@id': 'p3']],
                        "range"             : [
                                [
                                        "subClassOf": [
                                                [
                                                        "@type"     : "Restriction",
                                                        "onProperty": ["@id": "p4"],
                                                        "hasValue"  : ["@id": "https://id.kb.se/x"],
                                                ]
                                        ]
                                ]
                        ]
                ],
                [
                        '@id'               : 'restrictedP_p1_2',
                        '@type'             : 'DatatypeProperty',
                        'category'          : ['@id': "https://id.kb.se/vocab/shorthand"],
                        'propertyChainAxiom': [['@list': [
                                ['@id': '_:restrictedP'],
                                ['@id': 'p1']
                        ]]]
                ],
                [
                        '@id'        : 't1Type',
                        '@type'      : 'ObjectProperty',
                        'domain'     : ['@id': 'T1'],
                        'ls:indexKey': '@type'
                ],
                [
                        '@id'  : 'date',
                        '@type': 'DatatypeProperty',
                        'range': ['@id': 'xsd:dateTime']
                ],
                [
                        '@id'  : 'year',
                        '@type': 'DatatypeProperty'
                ]
        ]]
        def ctx = [
                '@context': [
                        'kbv'   : 'https://id.kb.se/vocab/',
                        '@vocab': 'https://id.kb.se/vocab/',
                        'p2'    : ['@type': '@vocab'],
                        'p4'    : ['@container': '@set'],
                        'p17'   : ['@container': '@set'],
                        'p18'   : ['@container': '@set']
                ]
        ]
        return new JsonLd(ctx, [:], vocab)
    }

    static def getEsMappings() {
        def mappings = [
                'properties': [
                        'p3'                                                 : ['type': 'nested'],
                        '@reverse.instanceOf.p3'                             : ['type': 'nested'],
                        'p15'                                                : ['type': 'nested', "include_in_parent": true],
                        '@type'                                              : ['type': 'keyword'],
                        'p2'                                                 : ['type': 'keyword'],
                        'p3.p4.@id'                                          : ['type': 'keyword'],
                        '_categoryByCollection.find.@id'                     : ['type': 'keyword'],
                        '_categoryByCollection.identify.@id'                 : ['type': 'keyword'],
                        '_categoryByCollection.@none.@id'                    : ['type': 'keyword'],
                        'instanceOf._categoryByCollection.find.@id'          : ['type': 'keyword'],
                        'instanceOf._categoryByCollection.identify.@id'      : ['type': 'keyword'],
                        'instanceOf._categoryByCollection.@none.@id'         : ['type': 'keyword'],
                        '@reverse.instanceOf._categoryByCollection.@none.@id': ['type': 'keyword'],
                        'date'                                               : ['type': 'date']
                ]
        ]
        // TODO
        return new ESMappings(List.of(mappings))
    }
}
